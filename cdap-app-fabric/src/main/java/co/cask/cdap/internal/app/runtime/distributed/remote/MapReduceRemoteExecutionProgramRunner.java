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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.distributed.MapReduceTwillProgramController;
import co.cask.cdap.internal.app.runtime.distributed.MapReduceTwillRunnable;
import co.cask.cdap.internal.app.runtime.distributed.ProgramLaunchConfig;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class MapReduceRemoteExecutionProgramRunner extends RemoteExecutionProgramRunner {

  @Inject
  MapReduceRemoteExecutionProgramRunner(CConfiguration cConf, Configuration hConf,
                                        @Constants.AppFabric.RemoteExecution TwillRunner twillRunner,
                                        Impersonator impersonator) {
    super(cConf, hConf, twillRunner, impersonator);
  }

  @Override
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);

    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.MAPREDUCE, "Only MapReduce process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getName());
  }

  @Override
  protected ProgramController createProgramController(TwillController twillController,
                                                      ProgramDescriptor programDescriptor, RunId runId) {
    return new MapReduceTwillProgramController(programDescriptor.getProgramId(), twillController, runId).startListen();
  }

  @Override
  protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                   CConfiguration cConf, Configuration hConf, File tempDir) throws IOException {

    ApplicationSpecification appSpec = program.getApplicationSpecification();
    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());

    // Get the resource for the container that runs the mapred client that will launch the actual mapred job.
    Map<String, String> clientArgs = RuntimeArguments.extractScope("task", "client",
                                                                   options.getUserArguments().asMap());
    // Add runnable. Only one instance for the MR driver
    launchConfig
      .addRunnable(spec.getName(), new MapReduceTwillRunnable(spec.getName()),
                   1, clientArgs, spec.getDriverResources(), 0)
      .addExtraClasspath(Collections.singletonList("$HADOOP_CLASSPATH"));
  }
}
