/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles states when a channel has been idle for a configured time interval, by closing the channel if an
 * HTTP Request is not in progress.
 */
public class IdleEventProcessor extends IdleStateHandler {

  private static final Logger LOG = LoggerFactory.getLogger(IdleEventProcessor.class);
  private boolean requestInProgress;

  public IdleEventProcessor(int allIdleTimeSeconds) {
    super(0, 0, allIdleTimeSeconds);
  }

  @Override
  public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception {
    if (IdleState.ALL_IDLE == e.state()) {
      if (requestInProgress) {
        LOG.trace("Request is in progress, so not closing channel.");
      } else {
        // No data has been sent or received for a while. Close channel.
        Channel channel = ctx.channel();
        channel.close();
        LOG.trace("No data has been sent or received for channel '{}' for more than the configured idle timeout. " +
                    "Closing the channel. Local Address: {}, Remote Address: {}",
                  channel, channel.localAddress(), channel.remoteAddress());
      }
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof LastHttpContent) {
      requestInProgress = false;
    }
    super.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    requestInProgress = true;
    super.write(ctx, msg, promise);
  }
}
