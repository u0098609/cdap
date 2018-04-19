/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.ssh.SSHConfig;
import co.cask.cdap.common.ssh.SSHProcess;
import co.cask.cdap.common.ssh.SSHSession;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import joptsimple.OptionSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.Arguments;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.LogOnlyEventHandler;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.json.ArgumentsCodec;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Dependencies;
import org.apache.twill.internal.utils.Paths;
import org.apache.twill.internal.utils.Resources;
import org.apache.twill.launcher.FindFreePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 *
 */
public class RemoteExecutionTwillPreparer implements TwillPreparer {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillPreparer.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TwillSpecification twillSpec;
  private final RunId runId;

  private final List<String> arguments = new ArrayList<>();
  private final Set<Class<?>> dependencies = Sets.newIdentityHashSet();
  private final List<URI> resources = new ArrayList<>();
  private final List<String> classPaths = new ArrayList<>();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();
  private final Map<String, Map<String, String>> environments = new HashMap<>();
  private final List<String> applicationClassPaths = new ArrayList<>();
  //private final Credentials credentials;
  private final Map<String, Map<String, String>> logLevels = new HashMap<>();
  private final LocationCache locationCache;
  private final Map<String, Integer> maxRetries = new HashMap<>();
  private final Map<String, Map<String, String>> runnableConfigs = new HashMap<>();
  private final Map<String, String> runnableExtraOptions = new HashMap<>();
  private final SSHConfig sshConfig;
  private final ProgramRunId programRunId;
  private final LocationFactory locationFactory;
  private final MessagingService messagingService;
  private String extraOptions;
  private JvmOptions.DebugOptions debugOptions;

  private ClassAcceptor classAcceptor;
  private String classLoaderClassName;

  RemoteExecutionTwillPreparer(CConfiguration cConf, Configuration hConf, SSHConfig sshConfig,
                               ProgramRunId programRunId, TwillSpecification twillSpec,
                               RunId runId, @Nullable String extraOptions,
                               LocationCache locationCache, LocationFactory locationFactory,
                               MessagingService messagingService) {
    this.debugOptions = JvmOptions.DebugOptions.NO_DEBUG;
    this.cConf = cConf;
    this.hConf = hConf;
    this.sshConfig = sshConfig;
    this.programRunId = programRunId;
    this.twillSpec = twillSpec;
    this.runId = runId;
    this.extraOptions = extraOptions == null ? "" : extraOptions;
    this.classAcceptor = new ClassAcceptor();
    this.locationCache = locationCache;
    this.locationFactory = locationFactory;
    this.messagingService = messagingService;
  }

  private void confirmRunnableName(String runnableName) {
    Preconditions.checkNotNull(runnableName);
    Preconditions.checkArgument(twillSpec.getRunnables().containsKey(runnableName),
                                "Runnable %s is not defined in the application.", runnableName);
  }

  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    for (Map.Entry<String, String> entry : config.entrySet()) {
      this.hConf.set(entry.getKey(), entry.getValue());
    }
    return this;
  }

  @Override
  public TwillPreparer withConfiguration(String runnableName, Map<String, String> config) {
    confirmRunnableName(runnableName);
    runnableConfigs.put(runnableName, Maps.newHashMap(config));
    return this;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    LOG.trace("LogHandler is not supported for {}", getClass().getSimpleName());
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    LOG.trace("Scheduler queue is not supported for {}", getClass().getSimpleName());
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.extraOptions = options;
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String runnableName, String options) {
    confirmRunnableName(runnableName);
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    runnableExtraOptions.put(runnableName, options);
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.extraOptions = extraOptions.isEmpty() ? options : extraOptions + " " + options;
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return enableDebugging(false, runnables);
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    for (String runnableName : runnables) {
      confirmRunnableName(runnableName);
    }
    this.debugOptions = new JvmOptions.DebugOptions(true, doSuspend, Arrays.asList(runnables));
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return withApplicationArguments(Arrays.asList(args));
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    Iterables.addAll(arguments, args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, Arrays.asList(args));
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    confirmRunnableName(runnableName);
    runnableArgs.putAll(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return withDependencies(Arrays.asList(classes));
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    Iterables.addAll(dependencies, classes);
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return withResources(Arrays.asList(resources));
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    Iterables.addAll(this.resources, resources);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return withClassPaths(Arrays.asList(classPaths));
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.classPaths, classPaths);
    return this;
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    // Add the given environments to all runnables
    for (String runnableName : twillSpec.getRunnables().keySet()) {
      setEnv(runnableName, env, false);
    }
    return this;
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    confirmRunnableName(runnableName);
    setEnv(runnableName, env, true);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    return withApplicationClassPaths(Arrays.asList(classPaths));
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.applicationClassPaths, classPaths);
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    this.classAcceptor = classAcceptor;
    return this;
  }

  @Override
  public TwillPreparer withMaxRetries(String runnableName, int maxRetries) {
    confirmRunnableName(runnableName);
    this.maxRetries.put(runnableName, maxRetries);
    return this;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    return setLogLevels(Collections.singletonMap(Logger.ROOT_LOGGER_NAME, logLevel));
  }

  @Override
  public TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels) {
    Preconditions.checkNotNull(logLevels);
    for (String runnableName : twillSpec.getRunnables().keySet()) {
      saveLogLevels(runnableName, logLevels);
    }
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> runnableLogLevels) {
    confirmRunnableName(runnableName);
    Preconditions.checkNotNull(runnableLogLevels);
    Preconditions.checkArgument(!(logLevels.containsKey(Logger.ROOT_LOGGER_NAME)
      && logLevels.get(Logger.ROOT_LOGGER_NAME) == null));
    saveLogLevels(runnableName, runnableLogLevels);
    return this;
  }

  @Override
  public TwillPreparer setClassLoader(String classLoaderClassName) {
    this.classLoaderClassName = classLoaderClassName;
    return this;
  }

  @Override
  public TwillController start() {
    return start(Constants.APPLICATION_MAX_START_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public TwillController start(long timeout, TimeUnit timeoutUnit) {
    try {
      Path tempDir = java.nio.file.Paths.get(cConf.get(co.cask.cdap.common.conf.Constants.CFG_LOCAL_DATA_DIR),
                                             cConf.get(co.cask.cdap.common.conf.Constants.AppFabric.TEMP_DIR))
                                        .toAbsolutePath();
      Path stagingDir = Files.createTempDirectory(tempDir, runId.getId());
      try {
        Map<String, LocalFile> localFiles = Maps.newHashMap();

        createLauncherJar(localFiles);
        createTwillJar(createBundler(classAcceptor, stagingDir), localFiles);
        createApplicationJar(createBundler(classAcceptor, stagingDir), localFiles);
        createResourcesJar(createBundler(classAcceptor, stagingDir), localFiles, stagingDir);

        TwillRuntimeSpecification twillRuntimeSpec;
        Path runtimeConfigDir = Files.createTempDirectory(stagingDir, Constants.Files.RUNTIME_CONFIG_JAR);
        try {
          twillRuntimeSpec = saveSpecification(twillSpec,
                                               runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC), stagingDir);
          saveLogback(runtimeConfigDir.resolve(Constants.Files.LOGBACK_TEMPLATE));
          saveClassPaths(runtimeConfigDir);
          saveArguments(new Arguments(arguments, runnableArgs), runtimeConfigDir.resolve(Constants.Files.ARGUMENTS));
          createRuntimeConfigJar(runtimeConfigDir, localFiles, stagingDir);
        } finally {
          Paths.deleteRecursively(runtimeConfigDir);
        }

        RuntimeSpecification runtimeSpec = twillRuntimeSpec.getTwillSpecification().getRunnables().values()
          .stream().findFirst().orElseThrow(IllegalStateException::new);

        try (SSHSession session = new SSHSession(sshConfig)) {
          // Upload files
          String targetPath = execute(session, "mkdir -p ./" + runId.getId(), "echo `pwd`/" + runId.getId()).trim();

          for (LocalFile localFile : Iterables.concat(localFiles.values(), runtimeSpec.getLocalFiles())) {
            LOG.debug("{} {} to {}@{}:{}/{}",
                      localFile.isArchive() ? "Upload and expand archive" : "Upload file",
                      localFile.getURI(), sshConfig.getUser(), sshConfig.getHost(), targetPath, localFile.getName());

            try (InputStream inputStream = openURI(localFile.getURI())) {
              if (localFile.isArchive()) {
                String expandedDir = targetPath + "/" + localFile.getName();
                execute(session, "mkdir -p " + expandedDir);
                String targetArchiveName = localFile.getName() + "." + Paths.getExtension(localFile.getName());
                //noinspection OctalInteger
                session.copy(inputStream, targetPath, targetArchiveName, localFile.getSize(), 0644,
                             localFile.getLastModified(), localFile.getLastModified());

                // Expand the archive
                // TODO: Support types other than jar
                execute(session, "cd " + expandedDir, String.format("jar xf %s/%s", targetPath, targetArchiveName));
              } else {
                //noinspection OctalInteger
                session.copy(inputStream, targetPath, localFile.getName(), localFile.getSize(), 0644,
                             localFile.getLastModified(), localFile.getLastModified());
              }
            }
          }

          JvmOptions jvmOptions = getJvmOptions();

          String runnableName = runtimeSpec.getName();
          int memory = Resources.computeMaxHeapSize(runtimeSpec.getResourceSpecification().getMemorySize(),
                                                    twillRuntimeSpec.getReservedMemory(runnableName),
                                                    twillRuntimeSpec.getMinHeapRatio(runnableName));

          String logsDir = targetPath + "/logs";

          // TODO: Will be removed Starting ZK
          execute(session, "sudo zookeeper-server start");

          StringWriter writer = new StringWriter();
          PrintWriter scriptWriter = new PrintWriter(writer, true);

          scriptWriter.println("#!/bin/bash");
          scriptWriter.println("export HADOOP_CLASSPATH=`hadoop classpath`");
          Map<String, String> runnableEnv = environments.getOrDefault(runnableName, Collections.emptyMap());
          for (Map.Entry<String, String> env : runnableEnv.entrySet()) {
            String value = env.getValue();
            if (ApplicationConstants.LOG_DIR_EXPANSION_VAR.equals(value)) {
              value = logsDir;
            }
            scriptWriter.printf("export %s=\"%s\"\n", env.getKey(), value);
          }
          scriptWriter.printf("export %s=\"%s\"\n", EnvKeys.TWILL_RUNNABLE_NAME, runnableName);
          scriptWriter.printf("mkdir -p %s/tmp\n", targetPath);
          scriptWriter.printf("mkdir -p %s\n", logsDir);
          scriptWriter.printf("cd %s\n", targetPath);
          scriptWriter.printf(
            "nohup java -Djava.io.tmpdir=tmp -cp %s/%s -Xmx%dm %s %s '%s' true >%s/stdout 2>%s/stderr &\n",
            targetPath, Constants.Files.LAUNCHER_JAR, memory,
            jvmOptions.getRunnableExtraOptions(runnableName),
            RemoteLauncher.class.getName(),
            runtimeSpec.getRunnableSpecification().getClassName(),
            logsDir, logsDir);

          scriptWriter.flush();

          byte[] scriptContent = writer.toString().getBytes(StandardCharsets.UTF_8);
          session.copy(new ByteArrayInputStream(scriptContent),
                       targetPath, "launcher.sh", scriptContent.length, 0700, null, null);

          LOG.info("Starting runnable {} with SSH on {}", runnableName, sshConfig.getHost());
          execute(session, "sudo " + targetPath + "/launcher.sh");

          ConnectionConfig connectionConfig = ConnectionConfig.builder()
            .setHostname(sshConfig.getHost())
            .setPort(443)   // TODO: Both hostname and port should be setup via DistributedProgramRunner
            .setSSLEnabled(true)
            .build();
          ClientConfig clientConfig = ClientConfig.builder()
            .setDefaultReadTimeout(60000)
            .setApiVersion("v1")
            .setVerifySSLCert(false)
            .setConnectionConfig(connectionConfig).build();
          RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId, cConf, messagingService, clientConfig);
          runtimeMonitor.start();
          return new RemoteExecutionTwillController(runId, runtimeMonitor);
        }
      } finally {
        DirUtils.deleteDirectoryContents(stagingDir.toFile(), false);
      }
    } catch (Exception e) {
      LOG.error("Failed to submit application {}", twillSpec.getName(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Same as calling {@link #execute(SSHSession, List)}.
   */
  private String execute(SSHSession session, String...commands) throws IOException {
    return execute(session, Arrays.asList(commands));
  }

  /**
   * Executes the given list of commands in the given {@link SSHSession}. If the command returned non-zero exit code,
   * a {@link RuntimeException} will be throw.
   *
   * @param session the ssh session for the commands to execute
   * @param commands the commands to execute
   * @return the output of the execution as string
   * @throws IOException if failed to execute the commands
   * @throws RuntimeException if the execution failed.
   */
  private String execute(SSHSession session, List<String> commands) throws IOException {
    SSHProcess process = session.execute(commands);

    // Reading will be blocked until the process finished
    String out = CharStreams.toString(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    String err = CharStreams.toString(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));

    if (process.exitValue() != 0) {
      throw new RuntimeException("Commands execution failed. Commands: " + commands
                                   + ", Output: " + out + " Error: " + err);
    }
    return out;
  }

  /**
   * Returns the extra options for the container JVM.
   */
  private String addClassLoaderClassName(String extraOptions) {
    if (classLoaderClassName == null) {
      return extraOptions;
    }
    String classLoaderProperty = "-D" + Constants.TWILL_CONTAINER_CLASSLOADER + "=" + classLoaderClassName;
    return extraOptions.isEmpty() ? classLoaderProperty : extraOptions + " " + classLoaderProperty;
  }

  private void setEnv(String runnableName, Map<String, String> env, boolean overwrite) {
    Map<String, String> environment = environments.get(runnableName);
    if (environment == null) {
      environment = new LinkedHashMap<>(env);
      environments.put(runnableName, environment);
      return;
    }

    for (Map.Entry<String, String> entry : env.entrySet()) {
      if (overwrite || !environment.containsKey(entry.getKey())) {
        environment.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private void saveLogLevels(String runnableName, Map<String, LogEntry.Level> logLevels) {
    Map<String, String> newLevels = new HashMap<>();
    for (Map.Entry<String, LogEntry.Level> entry : logLevels.entrySet()) {
      Preconditions.checkArgument(entry.getValue() != null,
                                  "Log level cannot be null for logger {}", entry.getKey());
      newLevels.put(entry.getKey(), entry.getValue().name());
    }
    this.logLevels.put(runnableName, newLevels);
  }

  private LocalFile createLocalFile(String name, Location location) throws IOException {
    return createLocalFile(name, location, false);
  }

  private LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }

  private void createTwillJar(final ApplicationBundler bundler,
                              Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.TWILL_JAR);
    Location location = locationCache.get(Constants.Files.TWILL_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        // TODO: based on the target cluster hadoop version to pick the right yarn-client version
        // We don't need it until we need to be able to run twill app in the target cluster
        bundler.createBundle(targetLocation, ApplicationMasterMain.class, TwillContainerMain.class, OptionSpec.class);
      }
    });

    LOG.debug("Done {}", Constants.Files.TWILL_JAR);
    localFiles.put(Constants.Files.TWILL_JAR, createLocalFile(Constants.Files.TWILL_JAR, location, true));
  }


  private void createApplicationJar(final ApplicationBundler bundler,
                                    Map<String, LocalFile> localFiles) throws IOException {
    try {
      final Set<Class<?>> classes = Sets.newIdentityHashSet();
      classes.addAll(dependencies);

      ClassLoader classLoader = getClassLoader();
      for (RuntimeSpecification spec : twillSpec.getRunnables().values()) {
        classes.add(classLoader.loadClass(spec.getRunnableSpecification().getClassName()));
      }

      // Add the TwillRunnableEventHandler class
      if (twillSpec.getEventHandler() != null) {
        classes.add(getClassLoader().loadClass(twillSpec.getEventHandler().getClassName()));
      }

      // The location name is computed from the MD5 of all the classes names
      // The localized name is always APPLICATION_JAR
      List<String> classList = classes.stream().map(Class::getName).collect(Collectors.toList());
      Collections.sort(classList);
      Hasher hasher = Hashing.md5().newHasher();
      for (String name : classList) {
        hasher.putString(name);
      }
      // Only depends on class list so that it can be reused across different launches
      String name = hasher.hash().toString() + "-" + Constants.Files.APPLICATION_JAR;

      LOG.debug("Create and copy {}", Constants.Files.APPLICATION_JAR);
      Location location = locationCache.get(name, new LocationCache.Loader() {
        @Override
        public void load(String name, Location targetLocation) throws IOException {
          bundler.createBundle(targetLocation, classes);
        }
      });

      LOG.debug("Done {}", Constants.Files.APPLICATION_JAR);

      localFiles.put(Constants.Files.APPLICATION_JAR,
                     createLocalFile(Constants.Files.APPLICATION_JAR, location, true));

    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  private void createResourcesJar(ApplicationBundler bundler, Map<String, LocalFile> localFiles,
                                  Path stagingDir) throws IOException {
    // If there is no resources, no need to create the jar file.
    if (resources.isEmpty()) {
      return;
    }

    LOG.debug("Create and copy {}", Constants.Files.RESOURCES_JAR);
    Location location = Locations.toLocation(Files.createTempFile(stagingDir, Constants.Files.RESOURCES_JAR, null));
    bundler.createBundle(location, Collections.emptyList(), resources);
    LOG.debug("Done {}", Constants.Files.RESOURCES_JAR);
    localFiles.put(Constants.Files.RESOURCES_JAR, createLocalFile(Constants.Files.RESOURCES_JAR, location, true));
  }

  private void createRuntimeConfigJar(Path dir, Map<String, LocalFile> localFiles,
                                      Path stagingDir) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.RUNTIME_CONFIG_JAR);

    // Jar everything under the given directory, which contains different files needed by AM/runnable containers
    Location location = Locations.toLocation(Files.createTempFile(stagingDir,
                                                                  Constants.Files.RUNTIME_CONFIG_JAR, null));
    try (
      JarOutputStream jarOutput = new JarOutputStream(location.getOutputStream());
      DirectoryStream<Path> stream = Files.newDirectoryStream(dir)
    ) {
      for (Path path : stream) {
        JarEntry jarEntry = new JarEntry(path.getFileName().toString());
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        jarEntry.setSize(attrs.size());
        jarEntry.setLastAccessTime(attrs.lastAccessTime());
        jarEntry.setLastModifiedTime(attrs.lastModifiedTime());
        jarOutput.putNextEntry(jarEntry);

        Files.copy(path, jarOutput);
        jarOutput.closeEntry();
      }
    }

    LOG.debug("Done {}", Constants.Files.RUNTIME_CONFIG_JAR);
    localFiles.put(Constants.Files.RUNTIME_CONFIG_JAR,
                   createLocalFile(Constants.Files.RUNTIME_CONFIG_JAR, location, true));
  }

  /**
   * Based on the given {@link TwillSpecification}, copy file to local filesystem.
   * @param spec The {@link TwillSpecification} for populating resource.
   */
  private Map<String, Collection<LocalFile>> populateRunnableLocalFiles(TwillSpecification spec,
                                                                        Path stagingDir) throws IOException {
    Map<String, Collection<LocalFile>> localFiles = new HashMap<>();

    LOG.debug("Populating Runnable LocalFiles");
    for (Map.Entry<String, RuntimeSpecification> entry: spec.getRunnables().entrySet()) {
      String runnableName = entry.getKey();

      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        localFiles.computeIfAbsent(runnableName, s -> new ArrayList<>()).add(resolveLocalFile(localFile, stagingDir));
      }
    }
    LOG.debug("Done Runnable LocalFiles");
    return localFiles;
  }

  private LocalFile resolveLocalFile(LocalFile localFile, Path stagingDir) throws IOException {
    URI uri = localFile.getURI();
    String scheme = uri.getScheme();

    // If local file, resolve the last modified time and the file size
    if (scheme == null || "file".equals(scheme)) {
      File file = new File(uri.getPath());
      return new DefaultLocalFile(localFile.getName(), uri, file.lastModified(),
                                  file.length(), localFile.isArchive(), localFile.getPattern());
    }

    // If have the same scheme as the location factory, resolve time and size using Location
    if (Objects.equals(locationFactory.getHomeLocation().toURI().getScheme(), scheme)) {
      Location location = locationFactory.create(uri);
      return new DefaultLocalFile(localFile.getName(), uri, location.lastModified(),
                                  location.length(), localFile.isArchive(), localFile.getPattern());
    }

    // For other cases, attempt to save the URI content to local file, using support URLSteamHandler
    try (InputStream input = uri.toURL().openStream()) {
      Path tempFile = Files.createTempFile(stagingDir, localFile.getName(), Paths.getExtension(localFile.getName()));
      Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
      BasicFileAttributes attrs = Files.readAttributes(tempFile, BasicFileAttributes.class);
      return new DefaultLocalFile(localFile.getName(), tempFile.toUri(), attrs.lastModifiedTime().toMillis(),
                                  attrs.size(), localFile.isArchive(), localFile.getPattern());
    }
  }

  private TwillRuntimeSpecification saveSpecification(TwillSpecification spec,
                                                      Path targetFile, Path stagingDir) throws IOException {
    final Map<String, Collection<LocalFile>> runnableLocalFiles = populateRunnableLocalFiles(spec, stagingDir);

    // Rewrite LocalFiles inside twillSpec
    Map<String, RuntimeSpecification> runtimeSpec = spec.getRunnables().entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> {
        RuntimeSpecification value = e.getValue();
        return new DefaultRuntimeSpecification(value.getName(), value.getRunnableSpecification(),
                                               value.getResourceSpecification(),
                                               runnableLocalFiles.getOrDefault(e.getKey(), Collections.emptyList()));
      }));

    // Serialize into a local temp file.
    LOG.debug("Creating {}", targetFile);
    try (Writer writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
      EventHandlerSpecification eventHandler = spec.getEventHandler();
      if (eventHandler == null) {
        eventHandler = new LogOnlyEventHandler().configure();
      }
      TwillSpecification newTwillSpec =
        new DefaultTwillSpecification(spec.getName(), runtimeSpec, spec.getOrders(),
                                      spec.getPlacementPolicies(), eventHandler);
      Map<String, String> configMap = Maps.newHashMap();
      for (Map.Entry<String, String> entry : hConf) {
        if (entry.getKey().startsWith("twill.")) {
          configMap.put(entry.getKey(), entry.getValue());
        }
      }

      TwillRuntimeSpecification twillRuntimeSpec = new TwillRuntimeSpecification(
        newTwillSpec, "", URI.create("."), "", runId, twillSpec.getName(),
        null,
        logLevels, maxRetries, configMap, runnableConfigs);
      TwillRuntimeSpecificationAdapter.create().toJson(twillRuntimeSpec, writer);
      LOG.debug("Done {}", targetFile);
      return twillRuntimeSpec;
    }
  }

  private void saveLogback(Path targetFile) throws IOException {
    URL url = getClass().getClassLoader().getResource(Constants.Files.LOGBACK_TEMPLATE);
    if (url == null) {
      return;
    }

    LOG.debug("Creating {}", targetFile);
    try (InputStream is = url.openStream()) {
      Files.copy(is, targetFile);
    }
    LOG.debug("Done {}", targetFile);
  }

  /**
   * Creates the launcher.jar for launch the main application.
   */
  private void createLauncherJar(Map<String, LocalFile> localFiles) throws URISyntaxException, IOException {

    LOG.debug("Create and copy {}", Constants.Files.LAUNCHER_JAR);

    Location location = locationCache.get(Constants.Files.LAUNCHER_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        // Create a jar file with the TwillLauncher and FindFreePort and dependent classes inside.
        try (JarOutputStream jarOut = new JarOutputStream(targetLocation.getOutputStream())) {
          ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
          if (classLoader == null) {
            classLoader = getClass().getClassLoader();
          }
          Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
            @Override
            public boolean accept(String className, URL classUrl, URL classPathUrl) {
              try {
                jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
                try (InputStream is = classUrl.openStream()) {
                  ByteStreams.copy(is, jarOut);
                }
              } catch (IOException e) {
                throw Throwables.propagate(e);
              }
              return true;
            }
          }, RemoteLauncher.class.getName(), FindFreePort.class.getName());
        }
      }
    });

    LOG.debug("Done {}", Constants.Files.LAUNCHER_JAR);

    localFiles.put(Constants.Files.LAUNCHER_JAR, createLocalFile(Constants.Files.LAUNCHER_JAR, location));
  }

  private void saveClassPaths(Path targetDir) throws IOException {
    Files.write(targetDir.resolve(Constants.Files.APPLICATION_CLASSPATH),
                Joiner.on(':').join(applicationClassPaths).getBytes(StandardCharsets.UTF_8));
    Files.write(targetDir.resolve(Constants.Files.CLASSPATH),
                Joiner.on(':').join(classPaths).getBytes(StandardCharsets.UTF_8));
  }

  private JvmOptions getJvmOptions() throws IOException {
    // Append runnable specific extra options.
    Map<String, String> runnableExtraOptions = this.runnableExtraOptions.entrySet()
      .stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> addClassLoaderClassName(extraOptions.isEmpty() ? e.getValue() : extraOptions + " " + e.getValue())));

    String globalOptions = addClassLoaderClassName(extraOptions);
    return new JvmOptions(globalOptions, runnableExtraOptions, debugOptions);
  }

  private void saveArguments(Arguments arguments, final Path targetPath) throws IOException {
    LOG.debug("Creating {}", targetPath);
    ArgumentsCodec.encode(arguments, () -> Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8));
    LOG.debug("Done {}", targetPath);
  }

  /**
   * Returns the context ClassLoader if there is any, otherwise, returns ClassLoader of this class.
   */
  private ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getClass().getClassLoader() : classLoader;
  }

  private ApplicationBundler createBundler(ClassAcceptor classAcceptor, Path stagingDir) {
    return new ApplicationBundler(classAcceptor).setTempDir(stagingDir.toFile());
  }

  private InputStream openURI(URI uri) throws IOException {
    String scheme = uri.getScheme();

    if (scheme == null || "file".equals(scheme)) {
      return new FileInputStream(uri.getPath());
    }

    // If having the same schema as the location factory, use the location factory to open the stream
    if (Objects.equals(locationFactory.getHomeLocation().toURI().getScheme(), scheme)) {
      return locationFactory.create(uri).getInputStream();
    }

    // Otherwise, fallback to using whatever supported in the JVM
    return uri.toURL().openStream();
  }
}
