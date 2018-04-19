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

package co.cask.cdap.app.guice;

import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.distributed.remote.MapReduceRemoteExecutionProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import co.cask.cdap.proto.ProgramType;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;

/**
 *
 */
final class RemoteExecutionProgramRunnerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(TwillRunnerService.class).annotatedWith(Constants.AppFabric.RemoteExecution.class)
      .to(RemoteExecutionTwillRunnerService.class).in(Scopes.SINGLETON);
    bind(TwillRunner.class).annotatedWith(Constants.AppFabric.RemoteExecution.class)
      .to(Key.get(TwillRunnerService.class, Constants.AppFabric.RemoteExecution.class));

    // Bind ProgramRunner
    MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
      binder(), ProgramType.class, ProgramRunner.class, Constants.AppFabric.RemoteExecution.class);

    defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(MapReduceRemoteExecutionProgramRunner.class);
  }
}
