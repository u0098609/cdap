/*
 * Copyright © 2014 Cask Data, Inc.
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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles requests to and from a discoverable endpoint.
 */
public class OutboundHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OutboundHandler.class);

  private final Channel inboundChannel;
  private boolean requestInProgress;

  public OutboundHandler(Channel inboundChannel) {
    this.inboundChannel = inboundChannel;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    inboundChannel.write(msg);
    if (msg instanceof LastHttpContent) {
      requestInProgress = false;
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    inboundChannel.flush();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    requestInProgress = true;
    ctx.write(msg, promise);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (requestInProgress) {
      final Channel channel = ctx.channel();
      ctx.executor().execute(new Runnable() {
        @Override
        public void run() {
          // If outboundChannel is not saturated anymore, continue accepting
          // the incoming traffic from the inboundChannel.
          if (channel.isWritable()) {
            LOG.trace("Setting inboundChannel readable.");
            inboundChannel.config().setAutoRead(true);
          } else {
            // If outboundChannel is saturated, do not read inboundChannel
            LOG.trace("Setting inboundChannel non-readable.");
            inboundChannel.config().setAutoRead(false);
          }
        }
      });
    }
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    // If the request is in progress and the outbound connection get dropped, close the inbound connection as well
    if (requestInProgress) {
      inboundChannel.close();
    }
    ctx.fireChannelUnregistered();
  }
}
