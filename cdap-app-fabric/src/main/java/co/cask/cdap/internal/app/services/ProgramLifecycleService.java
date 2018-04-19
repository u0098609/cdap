/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.LogLevelUpdater;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.app.store.profile.ProfileStore;
import co.cask.cdap.internal.provision.ProvisionerNotifier;
import co.cask.cdap.internal.provision.ProvisioningService;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.logging.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Programs.
 */
public class ProgramLifecycleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleService.class);

  private static final Gson GSON = ApplicationSpecificationAdapter
    .addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();

  private final Store store;
  private final ProfileStore profileStore;
  private final ProgramRuntimeService runtimeService;
  private final PropertiesResolver propertiesResolver;
  private final PreferencesStore preferencesStore;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final ProvisionerNotifier provisionerNotifier;
  private final ProvisioningService provisioningService;

  @Inject
  ProgramLifecycleService(Store store, ProfileStore profileStore, ProgramRuntimeService runtimeService,
                          PropertiesResolver propertiesResolver,
                          PreferencesStore preferencesStore, AuthorizationEnforcer authorizationEnforcer,
                          AuthenticationContext authenticationContext,
                          ProvisionerNotifier provisionerNotifier,
                          ProvisioningService provisioningService) {
    this.store = store;
    this.profileStore = profileStore;
    this.runtimeService = runtimeService;
    this.propertiesResolver = propertiesResolver;
    this.preferencesStore = preferencesStore;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.provisionerNotifier = provisionerNotifier;
    this.provisioningService = provisioningService;
  }

  /**
   * Returns the program status.
   * @param programId the id of the program for which the status call is made
   * @return the status of the program
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  public ProgramStatus getProgramStatus(ProgramId programId) throws Exception {
    // check that app exists
    ApplicationId appId = programId.getParent();
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new NotFoundException(appId);
    }

    return getExistingAppProgramStatus(appSpec, programId);
  }

  /**
   * Returns the program status with no need of application existence check.
   * @param appSpec the ApplicationSpecification of the existing application
   * @param programId the id of the program for which the status call is made
   * @return the status of the program
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  private ProgramStatus getExistingAppProgramStatus(ApplicationSpecification appSpec, ProgramId programId)
    throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());

    if (programId.getType() == ProgramType.WEBAPP) {
      throw new IllegalStateException("Webapp status is not supported");
    }

    ProgramSpecification spec = getExistingAppProgramSpecification(appSpec, programId);
    if (spec == null) {
      // program doesn't exist
      throw new NotFoundException(programId);
    }

    // TODO: CDAP-13296 read these runs in a single transaction
    // A program is RUNNING if there are any RUNNING run records
    if (!store.getRuns(programId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, 1).isEmpty()) {
      return ProgramStatus.RUNNING;
    }

    // A program is starting if there are any PENDING or STARTING run records and no RUNNING run records
    if (!store.getRuns(programId, ProgramRunStatus.STARTING, 0, Long.MAX_VALUE, 1).isEmpty() ||
      !store.getRuns(programId, ProgramRunStatus.PENDING, 0, Long.MAX_VALUE, 1).isEmpty()) {
      return ProgramStatus.STARTING;
    }
    return ProgramStatus.STOPPED;
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   *
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  @Nullable
  public ProgramSpecification getProgramSpecification(ProgramId programId) throws Exception {
    AuthorizationUtil.ensureOnePrivilege(programId, EnumSet.allOf(Action.class), authorizationEnforcer,
                                         authenticationContext.getPrincipal());
    ApplicationSpecification appSpec;
    appSpec = store.getApplication(programId.getParent());
    if (appSpec == null) {
      return null;
    }
    return getExistingAppProgramSpecification(appSpec, programId);
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   * @param appSpec the {@link ApplicationSpecification} of the existing application
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  private ProgramSpecification getExistingAppProgramSpecification(ApplicationSpecification appSpec, ProgramId programId)
    throws Exception {
    String programName = programId.getProgram();
    ProgramType type = programId.getType();
    ProgramSpecification programSpec;
    if (type == ProgramType.FLOW && appSpec.getFlows().containsKey(programName)) {
      programSpec = appSpec.getFlows().get(programName);
    } else if (type == ProgramType.MAPREDUCE && appSpec.getMapReduce().containsKey(programName)) {
      programSpec = appSpec.getMapReduce().get(programName);
    } else if (type == ProgramType.SPARK && appSpec.getSpark().containsKey(programName)) {
      programSpec = appSpec.getSpark().get(programName);
    } else if (type == ProgramType.WORKFLOW && appSpec.getWorkflows().containsKey(programName)) {
      programSpec = appSpec.getWorkflows().get(programName);
    } else if (type == ProgramType.SERVICE && appSpec.getServices().containsKey(programName)) {
      programSpec = appSpec.getServices().get(programName);
    } else if (type == ProgramType.WORKER && appSpec.getWorkers().containsKey(programName)) {
      programSpec = appSpec.getWorkers().get(programName);
    } else {
      programSpec = null;
    }
    return programSpec;
  }

  /**
   * Starts a Program with the specified argument overrides.
   *
   * @param programId the {@link ProgramId} to start/stop
   * @param overrides the arguments to override in the program's configured user arguments before starting
   * @param debug {@code true} if the program is to be started in debug mode, {@code false} otherwise
   * @return {@link RunId}
   * @throws ConflictException if the specified program is already running, and if concurrent runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link Action#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public synchronized RunId run(ProgramId programId, Map<String, String> overrides, boolean debug) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);
    if (isConcurrentRunsInSameAppForbidden(programId.getType()) && !isStoppedInSameProgram(programId)) {
      throw new ConflictException(String.format("Program %s is already running in an version of the same application",
                                                programId));
    }
    if (!isStopped(programId) && !isConcurrentRunsAllowed(programId.getType())) {
      throw new ConflictException(String.format("Program %s is already running", programId));
    }

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(Id.Program.fromEntityId(programId));
    Map<String, String> userArgs = propertiesResolver.getUserProperties(Id.Program.fromEntityId(programId));
    if (overrides != null) {
      userArgs.putAll(overrides);
    }
    return runInternal(programId, userArgs, sysArgs, debug);
  }


  /**
   * Runs a Program without authorization.
   *
   * Note that this method should only be called through internal service, it does not have auth check for starting the
   * program.
   *
   * @param programId the {@link ProgramId program} to run
   * @param userArgs user arguments
   * @param sysArgs system arguments
   * @param debug whether to start as a debug run
   * @return {@link RunId}
   * @throws IOException if there is an error starting the program
   * @throws NotFoundException if the namespace, application, or program is not found
   */
  public synchronized RunId runInternal(ProgramId programId, Map<String, String> userArgs,
                                        Map<String, String> sysArgs,
                                        boolean debug) throws NotFoundException, IOException {
    LOG.info("{} tries to run {} Program {}", authenticationContext.getPrincipal().getName(), programId.getType(),
             programId.getProgram());
    String scopedProfile = userArgs.get(SystemArguments.PROFILE_NAME);
    ProfileId profileId = scopedProfile == null ?
      ProfileId.DEFAULT : ProfileId.fromScopedName(programId.getNamespaceId(), scopedProfile);
    Profile profile = profileStore.getProfile(profileId);
    ProvisionerDetail spec = provisioningService.getProvisionerDetail(profile.getProvisioner().getName());
    if (spec == null) {
      throw new NotFoundException(String.format("Provisioner '%s' not found.", profile.getProvisioner().getName()));
    }
    // add profile properties to the system arguments
    Map<String, String> systemArgs = new HashMap<>();
    systemArgs.putAll(sysArgs);
    // get and add any user overrides for profile properties
    systemArgs.putAll(SystemArguments.getProfileProperties(userArgs));
    // add the rest of the profile properties to system properties
    SystemArguments.addProfileArgs(systemArgs, profile);

    // Set the ClusterMode. If it is DEFAULT profile, then it is ON_PREMISE, otherwise is ISOLATED
    // This should probably move into the provisioner later once we have a better contract for the
    // provisioner to actually pick what launching mechanism it wants to use.
    systemArgs.put(ProgramOptionConstants.CLUSTER_MODE,
                   (ProfileId.DEFAULT.equals(profileId) ? ClusterMode.ON_PREMISE : ClusterMode.ISOLATED).name());

    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArgs),
                                                             new BasicArguments(userArgs), debug);

    RunId runId = RunIds.generate();
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    String userId = SecurityRequestContext.getUserId();
    userId = userId == null ? "" : userId;
    provisionerNotifier.provisioning(programId.run(runId), programOptions, programDescriptor, userId);
    return runId;
  }


  /**
   * Starts a Program with the specified argument overrides, skipping cluster lifecycle steps in the run.
   *
   * @param programId the {@link ProgramId} to start/stop
   * @param overrides the arguments to override in the program's configured user arguments before starting
   * @param debug {@code true} if the program is to be started in debug mode, {@code false} otherwise
   * @return {@link ProgramController}
   * @throws ConflictException if the specified program is already running, and if concurrent runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link Action#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public synchronized ProgramController start(ProgramId programId, Map<String, String> overrides, boolean debug)
    throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);
    if (isConcurrentRunsInSameAppForbidden(programId.getType()) && !isStoppedInSameProgram(programId)) {
      throw new ConflictException(String.format("Program %s is already running in an version of the same application",
                                                programId));
    }
    if (!isStopped(programId) && !isConcurrentRunsAllowed(programId.getType())) {
      throw new ConflictException(String.format("Program %s is already running", programId));
    }

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(Id.Program.fromEntityId(programId));
    sysArgs.put(ProgramOptionConstants.SKIP_PROVISIONING, "true");
    Map<String, String> userArgs = propertiesResolver.getUserProperties(Id.Program.fromEntityId(programId));
    if (overrides != null) {
      userArgs.putAll(overrides);
    }

    BasicArguments systemArguments = new BasicArguments(sysArgs);
    BasicArguments userArguments = new BasicArguments(userArgs);
    ProgramOptions options = new SimpleProgramOptions(programId, systemArguments, userArguments, debug);
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    RunId runId = RunIds.generate();
    return startInternal(programDescriptor, options, programId.run(runId));
  }

  /**
   * Starts a Program with the specified argument overrides. Does not perform authorization checks, and is meant to
   * only be used by internal services. If the program is already started, returns the controller for the program.
   *
   * @param programDescriptor descriptor of the program to run
   * @param programOptions options for the program run
   * @param programRunId program run id
   * @return controller for the program
   * @throws IOException
   */
  public synchronized ProgramController startInternal(ProgramDescriptor programDescriptor,
                                                      ProgramOptions programOptions,
                                                      ProgramRunId programRunId) throws IOException {
    RunId runId = RunIds.fromString(programRunId.getRun());
    RuntimeInfo runtimeInfo = runtimeService.lookup(programRunId.getParent(), runId);
    if (runtimeInfo != null) {
      return runtimeInfo.getController();
    }

    runtimeInfo = runtimeService.run(programDescriptor, programOptions, runId);
    if (runtimeInfo == null) {
      throw new IOException(String.format("Failed to start program %s", programRunId));
    }
    return runtimeInfo.getController();
  }

  /**
   * Stops the specified program. The first run of the program as found by {@link ProgramRuntimeService} is stopped.
   *
   * @param programId the {@link ProgramId program} to stop
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws InterruptedException if there was a problem while waiting for the stop call to complete
   * @throws ExecutionException if there was a problem while waiting for the stop call to complete
   */
  public synchronized void stop(ProgramId programId) throws Exception {
    stop(programId, null);
  }

  /**
   * Stops the specified run of the specified program.
   *
   * @param programId the {@link ProgramId program} to stop
   * @param runId the runId of the program run to stop. If null, all runs of the program as returned by
   *              {@link ProgramRuntimeService} are stopped.
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws InterruptedException if there was a problem while waiting for the stop call to complete
   * @throws ExecutionException if there was a problem while waiting for the stop call to complete
   */
  public void stop(ProgramId programId, @Nullable String runId) throws Exception {
    List<ListenableFuture<ProgramController>> futures = issueStop(programId, runId);

    // Block until all stop requests completed. This call never throw ExecutionException
    Futures.successfulAsList(futures).get();

    Throwable failureCause = null;
    for (ListenableFuture<ProgramController> f : futures) {
      try {
        f.get();
      } catch (ExecutionException e) {
        // If the program is stopped in between the time listing runs and issuing stops of the program,
        // an IllegalStateException will be throw, which we can safely ignore
        if (!(e.getCause() instanceof IllegalStateException)) {
          if (failureCause == null) {
            failureCause = e.getCause();
          } else {
            failureCause.addSuppressed(e.getCause());
          }
        }
      }
    }
    if (failureCause != null) {
      throw new ExecutionException(String.format("%d out of %d runs of the program %s failed to stop",
                                                 failureCause.getSuppressed().length + 1, futures.size(), programId),
                                   failureCause);
    }
  }

  /**
   * Issues a command to stop the specified {@link RunId} of the specified {@link ProgramId} and returns a
   * {@link ListenableFuture} with the {@link ProgramController} for it.
   * Clients can wait for completion of the {@link ListenableFuture}.
   *
   * @param programId the {@link ProgramId program} to issue a stop for
   * @param runId the runId of the program run to stop. If null, all runs of the program as returned by
   *              {@link ProgramRuntimeService} are stopped.
   * @return a list of {@link ListenableFuture} with a {@link ProgramController} that clients can wait on for stop
   *         to complete.
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws UnauthorizedException if the user issuing the command is not authorized to stop the program. To stop a
   *                               program, a user requires {@link Action#EXECUTE} permission on the program.
   */
  public List<ListenableFuture<ProgramController>> issueStop(ProgramId programId,
                                                             @Nullable String runId) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);

    // See if the program is running as per the runtime service
    Map<RunId, RuntimeInfo> runtimeInfos = findRuntimeInfo(programId, runId);
    Map<ProgramRunId, RunRecordMeta> activeRunRecords = getActiveRuns(programId, runId);

    if (runtimeInfos.isEmpty() && activeRunRecords.isEmpty()) {
      // Error out if no run information from runtime service and from run record
      if (!store.applicationExists(programId.getParent())) {
        throw new ApplicationNotFoundException(programId.getParent());
      } else if (!store.programExists(programId)) {
        throw new ProgramNotFoundException(programId);
      }
      throw new BadRequestException(String.format("Program '%s' is not running.", programId));
    }

    // Stop the running program based on a combination of runtime info and run record
    // It's possible that some of them are not yet available from the runtimeService due to timing
    // differences between the run record was created vs being added to runtimeService
    // So we retry in a loop for up to 3 seconds max to cater for those cases

    Set<String> pendingStops = Stream.concat(runtimeInfos.keySet().stream().map(RunId::getId),
                                             activeRunRecords.keySet().stream().map(ProgramRunId::getRun))
                                      .collect(Collectors.toSet());

    List<ListenableFuture<ProgramController>> futures = new ArrayList<>();
    Stopwatch stopwatch = new Stopwatch().start();

    while (!pendingStops.isEmpty() && stopwatch.elapsedTime(TimeUnit.SECONDS) < 3L) {
      Iterator<String> iterator = pendingStops.iterator();
      while (iterator.hasNext()) {
        ProgramRunId activeRunId = programId.run(iterator.next());
        RunRecordMeta runRecord = activeRunRecords.get(activeRunId);
        if (runRecord == null) {
          runRecord = store.getRun(activeRunId);
        }
        // Check if the program is actually started from workflow and the workflow is running
        if (runRecord != null && runRecord.getProperties().containsKey("workflowrunid")
          && runRecord.getStatus().equals(ProgramRunStatus.RUNNING)) {
          String workflowRunId = runRecord.getProperties().get("workflowrunid");
          throw new BadRequestException(String.format("Cannot stop the program '%s' started by the Workflow " +
                                                        "run '%s'. Please stop the Workflow.", activeRunId,
                                                      workflowRunId));
        }

        RuntimeInfo runtimeInfo = runtimeService.lookup(programId, RunIds.fromString(activeRunId.getRun()));
        if (runtimeInfo != null) {
          futures.add(runtimeInfo.getController().stop());
          iterator.remove();
        }
      }

      if (!pendingStops.isEmpty()) {
        // If not able to stop all of them, meaning there are some runs that doesn't have a runtime info, due to
        // either the run was already finished or the run hasn't been registered with the runtime service.
        // We'll get the active runs again and filter it by the pending stops. Stop will be retried for those.
        Set<String> finalPendingStops = pendingStops;

        activeRunRecords = getActiveRuns(programId, runId).entrySet().stream()
          .filter(e -> finalPendingStops.contains(e.getKey().getRun()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        pendingStops = activeRunRecords.keySet().stream().map(ProgramRunId::getRun).collect(Collectors.toSet());

        if (!pendingStops.isEmpty()) {
          TimeUnit.MILLISECONDS.sleep(200);
        }
      }
    }

    return futures;
  }

  /**
   * Save runtime arguments for all future runs of this program. The runtime arguments are saved in the
   * {@link PreferencesStore}.
   *
   * @param programId the {@link ProgramId program} for which runtime arguments are to be saved
   * @param runtimeArgs the runtime arguments to save
   * @throws NotFoundException if the specified program was not found
   * @throws UnauthorizedException if the current user does not have sufficient privileges to save runtime arguments for
   *                               the specified program. To save runtime arguments for a program, a user requires
   *                               {@link Action#ADMIN} privileges on the program.
   */
  public void saveRuntimeArgs(ProgramId programId, Map<String, String> runtimeArgs) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (!store.programExists(programId)) {
      throw new NotFoundException(programId);
    }

    preferencesStore.setProperties(programId.getNamespace(), programId.getApplication(),
                                   programId.getType().getCategoryName(),
                                   programId.getProgram(), runtimeArgs);
  }

  /**
   * Gets runtime arguments for the program from the {@link PreferencesStore}
   *
   * @param programId the {@link ProgramId program} for which runtime arguments needs to be retrieved
   * @return {@link Map} containing runtime arguments of the program
   * @throws NotFoundException if the specified program was not found
   * @throws UnauthorizedException if the current user does not have sufficient privileges to get runtime arguments for
   * the specified program. To get runtime arguments for a program, a user requires
   * {@link Action#READ} privileges on the program.
   */
  public Map<String, String> getRuntimeArgs(@Name("programId") ProgramId programId) throws Exception {
    // user can have READ, ADMIN or EXECUTE to retrieve the runtime arguments
    AuthorizationUtil.ensureOnePrivilege(programId, EnumSet.of(Action.READ, Action.EXECUTE, Action.ADMIN),
                                         authorizationEnforcer, authenticationContext.getPrincipal());

    if (!store.programExists(programId)) {
      throw new NotFoundException(programId);
    }
    return preferencesStore.getProperties(programId.getNamespace(), programId.getApplication(),
                                          programId.getType().getCategoryName(), programId.getProgram());
  }

  /**
   * Update log levels for the given program. Only supported program types for this action are {@link ProgramType#FLOW},
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be updated
   * @param logLevels the {@link Map} of the log levels to be updated.
   * @param component the flowlet name. Only used when the program is a {@link ProgramType#FLOW flow}.
   * @param runId the run id of the program. {@code null} if update log levels for flowlet
   * @throws InterruptedException if there is an error while asynchronously updating log levels.
   * @throws ExecutionException if there is an error while asynchronously updating log levels.
   * @throws BadRequestException if the log level is not valid or the program type is not supported.
   * @throws UnauthorizedException if the user does not have privileges to update log levels for the specified program.
   *                               To update log levels for a program, a user needs {@link Action#ADMIN} on the program.
   */
  public void updateProgramLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
                                     @Nullable String component, @Nullable String runId) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (!EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(String.format("Updating log levels for program type %s is not supported",
                                                  programId.getType().getPrettyName()));
    }
    updateLogLevels(programId, logLevels, component, runId);
  }

  /**
   * Reset log levels for the given program. Only supported program types for this action are {@link ProgramType#FLOW},
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be reset.
   * @param loggerNames the {@link String} set of the logger names to be updated, empty means reset for all
   *                    loggers.
   * @param component the flowlet name. Only used when the program is a {@link ProgramType#FLOW flow}.
   * @param runId the run id of the program. {@code null} if set log levels for flowlet
   * @throws InterruptedException if there is an error while asynchronously resetting log levels.
   * @throws ExecutionException if there is an error while asynchronously resetting log levels.
   * @throws UnauthorizedException if the user does not have privileges to reset log levels for the specified program.
   *                               To reset log levels for a program, a user needs {@link Action#ADMIN} on the program.
   */
  public void resetProgramLogLevels(ProgramId programId, Set<String> loggerNames,
                                    @Nullable String component, @Nullable String runId) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (!EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(String.format("Resetting log levels for program type %s is not supported",
                                                  programId.getType().getPrettyName()));
    }
    resetLogLevels(programId, loggerNames, component, runId);
  }

  public boolean programExists(ProgramId programId) throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());
    return store.programExists(programId);
  }

  private boolean isStopped(ProgramId programId) throws Exception {
    return ProgramStatus.STOPPED == getProgramStatus(programId);
  }

  /**
   * Returns whether the given program is stopped in all versions of the app.
   * @param programId the id of the program for which the stopped status in all versions of the app is found
   * @return whether the given program is stopped in all versions of the app
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  private boolean isStoppedInSameProgram(ProgramId programId) throws Exception {
    // check that app exists
    Collection<ApplicationId> appIds = store.getAllAppVersionsAppIds(programId.getParent());
    if (appIds == null || appIds.isEmpty()) {
      throw new NotFoundException(Id.Application.from(programId.getNamespace(), programId.getApplication()));
    }
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    for (ApplicationId appId : appIds) {
      ProgramId pId = appId.program(programId.getType(), programId.getProgram());
      if (!getExistingAppProgramStatus(appSpec, pId).equals(ProgramStatus.STOPPED)) {
        return false;
      }
    }
    return true;
  }

  private boolean isConcurrentRunsInSameAppForbidden(ProgramType type) {
    // Concurrent runs in different (or same) versions of an application are forbidden for worker and flow
    return EnumSet.of(ProgramType.WORKER, ProgramType.FLOW).contains(type);
  }

  private boolean isConcurrentRunsAllowed(ProgramType type) {
    // Concurrent runs are only allowed for the Workflow, MapReduce and Spark
    return EnumSet.of(ProgramType.WORKFLOW, ProgramType.MAPREDUCE, ProgramType.SPARK).contains(type);
  }

  private Map<RunId, ProgramRuntimeService.RuntimeInfo> findRuntimeInfo(
    ProgramId programId, @Nullable String runId) throws BadRequestException {

    if (runId != null) {
      RunId run;
      try {
        run = RunIds.fromString(runId);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Error parsing run-id.", e);
      }
      ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.lookup(programId, run);
      return runtimeInfo == null ? Collections.emptyMap() : Collections.singletonMap(run, runtimeInfo);
    }
    return new HashMap<>(runtimeService.list(programId));
  }

  @Nullable
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId) throws BadRequestException {
    return findRuntimeInfo(programId, null).values().stream().findFirst().orElse(null);
  }

  /**
   * @see #setInstances(ProgramId, int, String)
   */
  public void setInstances(ProgramId programId, int instances) throws Exception {
    setInstances(programId, instances, null);
  }

  /**
   * Set instances for the given program. Only supported program types for this action are {@link ProgramType#FLOW},
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which instances are to be updated
   * @param instances the number of instances to be updated.
   * @param component the flowlet name. Only used when the program is a {@link ProgramType#FLOW flow}.
   * @throws InterruptedException if there is an error while asynchronously updating instances
   * @throws ExecutionException if there is an error while asynchronously updating instances
   * @throws BadRequestException if the number of instances specified is less than 0
   * @throws UnauthorizedException if the user does not have privileges to set instances for the specified program.
   *                               To set instances for a program, a user needs {@link Action#ADMIN} on the program.
   */
  public void setInstances(ProgramId programId, int instances, @Nullable String component) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (instances < 1) {
      throw new BadRequestException(String.format("Instance count should be greater than 0. Got %s.", instances));
    }
    switch (programId.getType()) {
      case SERVICE:
        setServiceInstances(programId, instances);
        break;
      case WORKER:
        setWorkerInstances(programId, instances);
        break;
      case FLOW:
        setFlowletInstances(programId, component, instances);
        break;
      default:
        throw new BadRequestException(String.format("Setting instances for program type %s is not supported",
                                                    programId.getType().getPrettyName()));
    }
  }

  /**
   * Lists all programs with the specified program type in a namespace. If perimeter security and authorization are
   * enabled, only returns the programs that the current user has access to.
   *
   * @param namespaceId the namespace to list datasets for
   * @return the programs in the provided namespace
   */
  public List<ProgramRecord> list(NamespaceId namespaceId, ProgramType type) throws Exception {
    Collection<ApplicationSpecification> appSpecs = store.getAllApplications(namespaceId);
    List<ProgramRecord> programRecords = new ArrayList<>();
    for (ApplicationSpecification appSpec : appSpecs) {
      switch (type) {
        case FLOW:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getFlows().values(), programRecords);
          break;
        case MAPREDUCE:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getMapReduce().values(), programRecords);
          break;
        case SPARK:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getSpark().values(), programRecords);
          break;
        case SERVICE:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getServices().values(), programRecords);
          break;
        case WORKER:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getWorkers().values(), programRecords);
          break;
        case WORKFLOW:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getWorkflows().values(), programRecords);
          break;
        default:
          throw new Exception("Unknown program type: " + type.name());
      }
    }
    return programRecords;
  }

  private void createProgramRecords(NamespaceId namespaceId, String appId, ProgramType type,
                                    Iterable<? extends ProgramSpecification> programSpecs,
                                    List<ProgramRecord> programRecords) throws Exception {
    for (ProgramSpecification programSpec : programSpecs) {
      if (hasAccess(namespaceId.app(appId).program(type, programSpec.getName()))) {
        programRecords.add(new ProgramRecord(type, appId, programSpec.getName(), programSpec.getDescription()));
      }
    }
  }

  private boolean hasAccess(ProgramId programId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    return !authorizationEnforcer.isVisible(Collections.singleton(programId), principal).isEmpty();
  }

  private void setWorkerInstances(ProgramId programId, int instances)
    throws ExecutionException, InterruptedException, BadRequestException {
    int oldInstances = store.getWorkerInstances(programId);
    if (oldInstances != instances) {
      store.setWorkerInstances(programId, instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                            ImmutableMap.of("runnable", programId.getProgram(),
                                                            "newInstances", String.valueOf(instances),
                                                            "oldInstances", String.valueOf(oldInstances))).get();
      }
    }
  }

  private void setFlowletInstances(ProgramId programId, String flowletId,
                                   int instances) throws ExecutionException, InterruptedException, BadRequestException {
    int oldInstances = store.getFlowletInstances(programId, flowletId);
    if (oldInstances != instances) {
      FlowSpecification flowSpec = store.setFlowletInstances(programId, flowletId, instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);
      if (runtimeInfo != null) {
        runtimeInfo.getController()
          .command(ProgramOptionConstants.INSTANCES,
                   ImmutableMap.of("flowlet", flowletId,
                                   "newInstances", String.valueOf(instances),
                                   "oldFlowSpec", GSON.toJson(flowSpec, FlowSpecification.class))).get();
      }
    }
  }

  private void setServiceInstances(ProgramId programId, int instances)
    throws ExecutionException, InterruptedException, BadRequestException {
    int oldInstances = store.getServiceInstances(programId);
    if (oldInstances != instances) {
      store.setServiceInstances(programId, instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                            ImmutableMap.of("runnable", programId.getProgram(),
                                                            "newInstances", String.valueOf(instances),
                                                            "oldInstances", String.valueOf(oldInstances))).get();
      }
    }
  }

  /**
   * Helper method to update log levels for Worker, Flow, or Service.
   */
  private void updateLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
                               @Nullable String component, @Nullable String runId) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values().stream()
                                                                                     .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.updateLogLevels(logLevels, component);
    }
  }

  /**
   * Helper method to reset log levels for Worker, Flow or Service.
   */
  private void resetLogLevels(ProgramId programId, Set<String> loggerNames,
                              @Nullable String component, @Nullable String runId) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values().stream()
                                                                                     .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.resetLogLevels(loggerNames, component);
    }
  }

  /**
   * Helper method to get the {@link LogLevelUpdater} for the program.
   */
  private LogLevelUpdater getLogLevelUpdater(RuntimeInfo runtimeInfo) throws Exception {
    ProgramController programController = runtimeInfo.getController();
    if (!(programController instanceof LogLevelUpdater)) {
      throw new BadRequestException("Update log levels at runtime is only supported in distributed mode");
    }
    return ((LogLevelUpdater) programController);
  }

  /**
   * Returns the active run records (STARTING / RUNNING / SUSPENDED) based on the given program id and an optional
   * run id.
   */
  private Map<ProgramRunId, RunRecordMeta> getActiveRuns(ProgramId programId, @Nullable String runId) {
    if (runId == null) {
      return store.getActiveRuns(programId);
    }
    RunRecordMeta runRecord = store.getRun(programId.run(runId));
    EnumSet<ProgramRunStatus> activeStates = EnumSet.of(ProgramRunStatus.STARTING,
                                                        ProgramRunStatus.RUNNING,
                                                        ProgramRunStatus.SUSPENDED);
    return runRecord == null || !activeStates.contains(runRecord.getStatus())
      ? Collections.emptyMap()
      : Collections.singletonMap(programId.run(runId), runRecord);
  }
}
