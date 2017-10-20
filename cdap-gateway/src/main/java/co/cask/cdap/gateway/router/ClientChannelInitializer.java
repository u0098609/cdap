/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.router.handlers.AuditLogHandler;
import co.cask.cdap.gateway.router.handlers.IdleEventProcessor;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;

/**
 * Constructs a pipeline factory to be used for creating client pipelines.
 */
class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final CConfiguration cConf;
  private final int connectionTimeout;

  ClientChannelInitializer(CConfiguration cConf, int connectionTimeout) {
    this.cConf = cConf;
    this.connectionTimeout = connectionTimeout;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("codec", new HttpClientCodec());
    pipeline.addLast("idle-event-processor", new IdleEventProcessor(connectionTimeout));

    if (cConf.getBoolean(Constants.Router.ROUTER_AUDIT_LOG_ENABLED)) {
      pipeline.addLast("audit-log", new AuditLogHandler(cConf));
    }
  }
}
