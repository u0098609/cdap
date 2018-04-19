/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import ch.qos.logback.classic.Level;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.logging.LoggerLogHandler;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.twill.TwillAppLifecycleEventHandler;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.LocalizationUtils;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.store.SecureStoreUtils;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TxConstants;
import org.apache.twill.api.Configs;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;


/**
 * Defines the base framework for starting {@link Program} in the cluster.
 */
public abstract class DistributedProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRunner.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();
  private static final String HADOOP_CONF_FILE_NAME = "hConf.xml";
  private static final String CDAP_CONF_FILE_NAME = "cConf.xml";
  private static final String APP_SPEC_FILE_NAME = "appSpec.json";
  private static final String LOGBACK_FILE_NAME = "logback.xml";
  private static final String PROGRAM_OPTIONS_FILE_NAME = "program.options.json";

  protected final YarnConfiguration hConf;
  protected final CConfiguration cConf;
  protected final TokenSecureStoreRenewer secureStoreRenewer;
  private final TwillRunner twillRunner;
  private final Impersonator impersonator;

  protected DistributedProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                     TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                     Impersonator impersonator) {
    this.twillRunner = twillRunner;
    this.hConf = hConf;
    this.cConf = cConf;
    this.secureStoreRenewer = tokenSecureStoreRenewer;
    this.impersonator = impersonator;
  }

  protected EventHandler createEventHandler(CConfiguration cConf, ProgramOptions options) {
    return new TwillAppLifecycleEventHandler(cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE),
                                             options);
  }

  /**
   * Validates the options for the program.
   * Subclasses can override this to also validate the options for their sub-programs.
   */
  protected void validateOptions(Program program, ProgramOptions options) {
    // this will throw an exception if the custom tx timeout is invalid
    SystemArguments.validateTransactionTimeout(options.getUserArguments().asMap(), cConf);
  }


  /**
   * Creates a {@link ProgramController} for the given program that was launched as a Twill application.
   *
   * @param twillController the {@link TwillController} to interact with the twill application
   * @param programDescriptor information for the Program being launched
   * @param runId the run id of the particular execution
   * @return a new instance of {@link ProgramController}.
   */
  protected abstract ProgramController createProgramController(TwillController twillController,
                                                               ProgramDescriptor programDescriptor, RunId runId);

  /**
   * Provides the configuration for launching an program container.
   *
   * @param launchConfig the {@link ProgramLaunchConfig} to setup
   * @param program the program to launch
   * @param options the program options
   * @param cConf the configuration for this launch
   * @param hConf the hadoop configuration for this launch
   * @param tempDir a temporary directory for creating temp file. The content will be cleanup automatically
   *                once the program is launch.
   */
  protected abstract void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                            CConfiguration cConf, Configuration hConf, File tempDir) throws IOException;

  /**
   * The extra hook to be called right before the program is launch. This method will be called with
   * user impersonation.
   *
   * @param program the program to launch
   * @param options the program options
   */
  protected void beforeLaunch(Program program, ProgramOptions options) {
    // no-op
  }

  @Override
  public final ProgramController run(final Program program, ProgramOptions oldOptions) {
    validateOptions(program, oldOptions);

    final CConfiguration cConf = createContainerCConf(this.cConf);
    final Configuration hConf = createContainerHConf(this.hConf);

    final File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                         cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      final ProgramLaunchConfig launchConfig = new ProgramLaunchConfig();
      setupLaunchConfig(launchConfig, program, oldOptions, cConf, hConf, tempDir);

      // Add extra localize resources needed by the program runner
      final Map<String, LocalizeResource> localizeResources = new HashMap<>(launchConfig.getExtraResources());
      final List<String> additionalClassPaths = new ArrayList<>();
      addContainerJars(cConf, localizeResources, additionalClassPaths);

      prepareHBaseDDLExecutorResources(tempDir, cConf, localizeResources);

      // Save the configuration to files
      File hConfFile = saveHConf(hConf, new File(tempDir, HADOOP_CONF_FILE_NAME));
      localizeResources.put(HADOOP_CONF_FILE_NAME, new LocalizeResource(hConfFile));

      File cConfFile = saveCConf(cConf, new File(tempDir, CDAP_CONF_FILE_NAME));
      localizeResources.put(CDAP_CONF_FILE_NAME, new LocalizeResource(cConfFile));

      // Localize the program jar
      Location programJarLocation = program.getJarLocation();
      final String programJarName = programJarLocation.getName();
      localizeResources.put(programJarName, new LocalizeResource(program.getJarLocation().toURI(), false));

      // Localize an expanded program jar
      final String expandedProgramJarName = "expanded." + programJarName;
      localizeResources.put(expandedProgramJarName, new LocalizeResource(program.getJarLocation().toURI(), true));

      // Localize the app spec
      localizeResources.put(APP_SPEC_FILE_NAME,
                            new LocalizeResource(saveJsonFile(program.getApplicationSpecification(),
                                                              ApplicationSpecification.class,
                                                              File.createTempFile("appSpec", ".json", tempDir))));

      final URI logbackURI = getLogBackURI(program, tempDir);
      if (logbackURI != null) {
        // Localize the logback xml
        localizeResources.put(LOGBACK_FILE_NAME, new LocalizeResource(logbackURI, false));
      }

      // Update the ProgramOptions to carry program and runtime information necessary to reconstruct the program
      // and runs it in the remote container
      ProgramOptions options = updateProgramOptions(
        oldOptions, localizeResources, DirUtils.createTempDir(tempDir),
          ProgramOptionConstants.PROGRAM_JAR, programJarName,
          ProgramOptionConstants.EXPANDED_PROGRAM_JAR, expandedProgramJarName,
          ProgramOptionConstants.HADOOP_CONF_FILE, HADOOP_CONF_FILE_NAME,
          ProgramOptionConstants.CDAP_CONF_FILE, CDAP_CONF_FILE_NAME,
          ProgramOptionConstants.APP_SPEC_FILE, APP_SPEC_FILE_NAME
        );

      // Localize the serialized program options
      localizeResources.put(PROGRAM_OPTIONS_FILE_NAME,
                            new LocalizeResource(saveJsonFile(
                              options, ProgramOptions.class,
                              File.createTempFile("program.options", ".json", tempDir))));

      Callable<ProgramController> callable = new Callable<ProgramController>() {
        @Override
        public ProgramController call() throws Exception {
          ProgramRunId programRunId = program.getId().run(ProgramRunners.getRunId(options));
          ProgramTwillApplication twillApplication = new ProgramTwillApplication(programRunId, options,
                                                                                 launchConfig.getRunnables(),
                                                                                 launchConfig.getLaunchOrder(),
                                                                                 localizeResources,
                                                                                 createEventHandler(cConf, options));
          TwillPreparer twillPreparer = twillRunner.prepare(twillApplication);

          // Also add the configuration files to container classpath so that the
          // TwillAppLifecycleEventHandler can get it. This can be removed when TWILL-246 is fixed
          twillPreparer.withResources(hConfFile.toURI(), cConfFile.toURI());

          Map<String, String> userArgs = options.getUserArguments().asMap();

          // Setup log level
          twillPreparer.setLogLevels(transformLogLevels(SystemArguments.getLogLevels(userArgs)));

          // Set the configuration for the twill application
          Map<String, String> twillConfigs = new HashMap<>();
          if (DistributedProgramRunner.this instanceof LongRunningDistributedProgramRunner) {
            twillConfigs.put(Configs.Keys.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL,
                             cConf.get(Constants.AppFabric.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL));
          }
          // Add the one from the runtime arguments
          twillConfigs.putAll(SystemArguments.getTwillApplicationConfigs(userArgs));
          twillPreparer.withConfiguration(twillConfigs);

          // Setup per runnable configurations
          for (Map.Entry<String, RunnableDefinition> entry : launchConfig.getRunnables().entrySet()) {
            String runnable = entry.getKey();
            RunnableDefinition runnableDefinition = entry.getValue();
            if (runnableDefinition.getMaxRetries() != null) {
              twillPreparer.withMaxRetries(runnable, runnableDefinition.getMaxRetries());
            }
            twillPreparer.setLogLevels(runnable, transformLogLevels(runnableDefinition.getLogLevels()));
            twillPreparer.withConfiguration(runnable, runnableDefinition.getTwillRunnableConfigs());
          }

          if (options.isDebug()) {
            twillPreparer.enableDebugging();
          }

          logProgramStart(program, options);

          LOG.info("Starting {} with debugging enabled: {}, logback: {}",
                   program.getId(), options.isDebug(), logbackURI);

          // Add scheduler queue name if defined
          String schedulerQueueName = options.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
          if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
            LOG.info("Setting scheduler queue for app {} as {}", program.getId(), schedulerQueueName);
            twillPreparer.setSchedulerQueue(schedulerQueueName);
          }

          if (logbackURI != null) {
            twillPreparer.addJVMOptions("-Dlogback.configurationFile=" + LOGBACK_FILE_NAME);
          }

          String logLevelConf = cConf.get(Constants.COLLECT_APP_CONTAINER_LOG_LEVEL).toUpperCase();
          if ("OFF".equals(logLevelConf)) {
            twillPreparer.withConfiguration(Collections.singletonMap(Configs.Keys.LOG_COLLECTION_ENABLED, "false"));
          } else {
            LogEntry.Level logLevel = LogEntry.Level.ERROR;
            if ("ALL".equals(logLevelConf)) {
              logLevel = LogEntry.Level.TRACE;
            } else {
              try {
                logLevel = LogEntry.Level.valueOf(logLevelConf.toUpperCase());
              } catch (Exception e) {
                LOG.warn("Invalid application container log level {}. Defaulting to ERROR.", logLevelConf);
              }
            }
            twillPreparer.addLogHandler(new LoggerLogHandler(LOG, logLevel));
          }

          // Add secure tokens
          if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
            twillPreparer.addSecureStore(YarnSecureStore.create(secureStoreRenewer.createCredentials()));
          }

          // Setup the environment for the container logback.xml
          twillPreparer.withEnv(Collections.singletonMap("CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR));

          // Add dependencies
          Set<Class<?>> extraDependencies = new HashSet<>(launchConfig.getExtraDependencies());
          extraDependencies.add(HBaseTableUtilFactory.getHBaseTableUtilClass(cConf));
          if (SecureStoreUtils.isKMSBacked(cConf) && SecureStoreUtils.isKMSCapable()) {
            extraDependencies.add(SecureStoreUtils.getKMSSecureStore());
          }
          twillPreparer.withDependencies(extraDependencies);

          // Add the additional classes to the classpath that comes from the container jar setting
          twillPreparer.withClassPaths(additionalClassPaths);

          twillPreparer.withClassPaths(launchConfig.getExtraClasspath());
          twillPreparer.withEnv(launchConfig.getExtraEnv());

          // The Yarn app classpath goes last
          List<String> yarnAppClassPath = Arrays.asList(
            hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
          twillPreparer
            .withApplicationClassPaths(yarnAppClassPath)
            .withClassPaths(yarnAppClassPath)
            .withBundlerClassAcceptor(launchConfig.getClassAcceptor())
            .withApplicationArguments(PROGRAM_OPTIONS_FILE_NAME)
            // Use the MainClassLoader for class rewriting
            .setClassLoader(MainClassLoader.class.getName());

          // Invoke the before launch hook
          beforeLaunch(program, options);

          TwillController twillController;
          // Change the context classloader to the combine classloader of this ProgramRunner and
          // all the classloaders of the dependencies classes so that Twill can trace classes.
          ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
            DistributedProgramRunner.this.getClass().getClassLoader(),
            extraDependencies.stream().map(Class::getClassLoader)::iterator));
          try {
            twillController = twillPreparer.start(cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS),
                                                  TimeUnit.SECONDS);
          } finally {
            ClassLoaders.setContextClassLoader(oldClassLoader);
          }
          return createProgramController(addCleanupListener(twillController, program, tempDir),
                                         new ProgramDescriptor(program.getId(), program.getApplicationSpecification()),
                                         ProgramRunners.getRunId(options));
        }
      };

      return impersonator.doAs(program.getId(), callable);

    } catch (Exception e) {
      deleteDirectory(tempDir);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates the {@link CConfiguration} to be used in the program container.
   */
  private CConfiguration createContainerCConf(CConfiguration cConf) {
    CConfiguration result = CConfiguration.copy(cConf);
    // don't have tephra retry in order to give CDAP more control over when to retry and how.
    result.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
    result.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 0);

    // Unset the hbase.client.retries.number and hbase.rpc.timeout so that program container
    // runs with default values for them from hbase-site/hbase-default.
    result.unset(Constants.HBase.CLIENT_RETRIES);
    result.unset(Constants.HBase.RPC_TIMEOUT);

    // Unset the runtime extension directory as the necessary extension jars should be shipped to the container
    // by the distributed ProgramRunner.
    result.unset(Constants.AppFabric.RUNTIME_EXT_DIR);

    // Set the CFG_LOCAL_DATA_DIR to a relative path as the data directory for the container should be relative to the
    // container directory
    result.set(Constants.CFG_LOCAL_DATA_DIR, "data");

    return result;
  }

  /**
   * Creates the {@link Configuration} to be used in the program container.
   */
  private Configuration createContainerHConf(Configuration hConf) {
    Configuration result = new YarnConfiguration(hConf);
    // Unset the hbase.client.retries.number and hbase.rpc.timeout so that program container
    // runs with default values for them from hbase-site/hbase-default.
    result.unset(Constants.HBase.CLIENT_RETRIES);
    result.unset(Constants.HBase.RPC_TIMEOUT);

    return result;
  }

  /**
   * Adds extra jars as defined in {@link CConfiguration} to the container and container classpath.
   */
  private void addContainerJars(CConfiguration cConf,
                                Map<String, LocalizeResource> localizeResources, Collection<String> classpath) {
    // Create localize resources and update classpath for the container based on the "program.container.dist.jars"
    // configuration.
    List<String> containerExtraJars = new ArrayList<>();
    for (URI jarURI : CConfigurationUtil.getExtraJars(cConf)) {
      String scheme = jarURI.getScheme();
      LocalizeResource localizeResource = new LocalizeResource(jarURI, false);
      String localizedName = LocalizationUtils.getLocalizedName(jarURI);
      localizeResources.put(localizedName, localizeResource);
      classpath.add(localizedName);
      String jarPath = "file".equals(scheme) ? localizedName : jarURI.toString();
      containerExtraJars.add(jarPath);
    }

    // Set the "program.container.dist.jars" since files are already localized to the container.
    cConf.setStrings(Constants.AppFabric.PROGRAM_CONTAINER_DIST_JARS,
                     containerExtraJars.toArray(new String[containerExtraJars.size()]));
  }

  /**
   * Set up Logging Context so the Log is tagged correctly for the Program.
   * Reset the context once done.
   */
  private void logProgramStart(Program program, ProgramOptions options) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(program.getNamespaceId(), program.getApplicationId(),
                                             program.getName(), program.getType(),
                                             ProgramRunners.getRunId(options).getId(),
                                             options.getArguments().asMap());
    Cancellable saveContextCancellable =
      LoggingContextAccessor.setLoggingContext(loggingContext);
    String userArguments = Joiner.on(", ").withKeyValueSeparator("=").join(options.getUserArguments());
    LOG.info("Starting {} Program '{}' with Arguments [{}]", program.getType(), program.getName(), userArguments);
    saveContextCancellable.cancel();
  }

  /**
   * Creates a new instance of {@link ProgramOptions} with artifact localization information and with
   * extra system arguments, while maintaining other fields of the given {@link ProgramOptions}.
   *
   * @param options the original {@link ProgramOptions}.
   * @param localizeResources a {@link Map} of {@link LocalizeResource} to be localized to the remote container
   * @param tempDir a local temporary directory for creating files for artifact localization.
   * @param extraSystemArgs a set of extra system arguments to be added/updated
   * @return a new instance of {@link ProgramOptions}
   * @throws IOException if failed to create local copy of artifact files
   */
  private ProgramOptions updateProgramOptions(ProgramOptions options,
                                              Map<String, LocalizeResource> localizeResources,
                                              File tempDir, String...extraSystemArgs) throws IOException {
    if (extraSystemArgs.length % 2 != 0) {
      // This shouldn't happen.
      throw new IllegalArgumentException("Number of extra system arguments must be even in the form of k1,v1,k2,v2...");
    }

    Arguments systemArgs = options.getArguments();

    Map<String, String> newSystemArgs = new HashMap<>(systemArgs.asMap());
    for (int i = 0; i < extraSystemArgs.length; i += 2) {
      newSystemArgs.put(extraSystemArgs[i], extraSystemArgs[i + 1]);
    }

    if (systemArgs.hasOption(ProgramOptionConstants.PLUGIN_DIR)) {
      File localDir = new File(systemArgs.getOption(ProgramOptionConstants.PLUGIN_DIR));
      File archiveFile = new File(tempDir, "artifacts.jar");
      BundleJarUtil.createJar(localDir, archiveFile);

      // Localize plugins to two files, one expanded into a directory, one not.
      localizeResources.put("artifacts", new LocalizeResource(archiveFile, true));
      localizeResources.put("artifacts_archive.jar", new LocalizeResource(archiveFile, false));

      newSystemArgs.put(ProgramOptionConstants.PLUGIN_DIR, "artifacts");
      newSystemArgs.put(ProgramOptionConstants.PLUGIN_ARCHIVE, "artifacts_archive.jar");

    }
    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(newSystemArgs),
                                    options.getUserArguments(), options.isDebug());
  }

  /**
   * Returns a {@link URI} for the logback.xml file to be localized to container and available in the container
   * classpath.
   */
  @Nullable
  private URI getLogBackURI(Program program, File tempDir) throws IOException, URISyntaxException {
    URL logbackURL = program.getClassLoader().getResource("logback.xml");
    if (logbackURL != null) {
      return logbackURL.toURI();
    }
    URL resource = getClass().getClassLoader().getResource("logback-container.xml");
    if (resource == null) {
      return null;
    }
    // Copy the template
    File logbackFile = new File(tempDir, "logback.xml");
    try (InputStream is = resource.openStream()) {
      Files.copy(is, logbackFile.toPath());
    }
    return logbackFile.toURI();
  }

  private Map<String, LogEntry.Level> transformLogLevels(Map<String, Level> logLevels) {
    return Maps.transformValues(logLevels, new Function<Level, LogEntry.Level>() {
      @Override
      public LogEntry.Level apply(Level level) {
        // Twill LogEntry.Level doesn't have ALL and OFF, so map them to lowest and highest log level respectively
        if (level.equals(Level.ALL)) {
          return LogEntry.Level.TRACE;
        }
        if (level.equals(Level.OFF)) {
          return LogEntry.Level.FATAL;
        }
        return LogEntry.Level.valueOf(level.toString());
      }
    });
  }

  private File saveCConf(CConfiguration cConf, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      cConf.writeXml(writer);
    }
    return file;
  }

  private File saveHConf(Configuration conf, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  private <T> File saveJsonFile(T obj, Class<T> type, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      GSON.toJson(obj, type, writer);
    }
    return file;
  }

  /**
   * Deletes the given directory recursively. Only log if there is {@link IOException}.
   */
  private void deleteDirectory(File directory) {
    try {
      DirUtils.deleteDirectoryContents(directory);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", directory, e);
    }
  }

  /**
   * Adds a listener to the given TwillController to delete local temp files when the program has started/terminated.
   * The local temp files could be removed once the program is started, since Twill would keep the files in
   * HDFS and no long needs the local temp files once program is started.
   *
   * @return The same TwillController instance.
   */
  private TwillController addCleanupListener(TwillController controller,
                                             final Program program, final File tempDir) {

    final AtomicBoolean deleted = new AtomicBoolean(false);
    Runnable cleanup = () -> {
      if (!deleted.compareAndSet(false, true)) {
        return;
      }
      LOG.debug("Cleanup tmp files for {}: {}", program.getId(), tempDir);
      deleteDirectory(tempDir);
    };
    controller.onRunning(cleanup, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(cleanup, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }

  /**
   * Prepares the {@link HBaseDDLExecutor} implementation for localization.
   */
  private void prepareHBaseDDLExecutorResources(File tempDir, CConfiguration cConf,
                                                Map<String, LocalizeResource> localizeResources) throws IOException {
    String ddlExecutorExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
    if (ddlExecutorExtensionDir == null) {
      // Nothing to localize
      return;
    }

    final File target = new File(tempDir, "hbaseddlext.jar");
    BundleJarUtil.createJar(new File(ddlExecutorExtensionDir), target);
    localizeResources.put(target.getName(), new LocalizeResource(target, true));
    cConf.set(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, target.getName());
  }
}
