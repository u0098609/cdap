/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.gateway.router.RouterServiceLookup;
import com.google.common.collect.Queues;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLException;

/**
 * A {@link ChannelInboundHandler} for forwarding incoming request to appropriate CDAP service endpoint
 * based on the request.
 */
public class HttpRequestRouter extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestRouter.class);
  private static final byte[] HTTPS_SCHEME_BYTES = Constants.Security.SSL_URI_SCHEME.getBytes();

  private final Bootstrap clientBootstrap;
  private final RouterServiceLookup serviceLookup;
  private final Map<Discoverable, Queue<MessageSender>> messageSenders;
  private int inflightRequests;
  private MessageSender currentMessageSender;

  public HttpRequestRouter(Bootstrap clientBootstrap, RouterServiceLookup serviceLookup) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.messageSenders = new HashMap<>();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    try {
      final ChannelPromise promise = ctx.newPromise();
      final Channel inboundChannel = ctx.channel();

      if (msg instanceof HttpRequest) {
        inflightRequests++;
        if (inflightRequests != 1) {
          // This means there is concurrent request via HTTP pipelining.
          // Simply return
          // At the end of the first response, we'll response to all the other requests as well
          return;
        }

        // Disable read until sending of this request object is completed successfully
        // This is for handling the initial connection delay
        inboundChannel.config().setAutoRead(false);
        promise.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              inboundChannel.config().setAutoRead(true);
            }
          }
        });

        HttpRequest request = (HttpRequest) msg;
        currentMessageSender = getMessageSender(
          inboundChannel, getDiscoverable(request, (InetSocketAddress) inboundChannel.localAddress())
        );
      }

      if (inflightRequests == 1) {
        promise.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
              inboundChannel.writeAndFlush(createErrorResponse(future.cause()))
                .addListener(ChannelFutureListener.CLOSE);
            }
          }
        });

        ReferenceCountUtil.retain(msg);
        ctx.executor().execute(new Runnable() {
          @Override
          public void run() {
            MessageSender sender = currentMessageSender;
            if (sender != null) {
              sender.send(msg, promise);
            } else {
              ReferenceCountUtil.release(msg);
            }
          }
        });
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    ctx.writeAndFlush(msg, promise);
    inflightRequests--;

    // When the response for the first request is completed, write N responses for all pipelining requests (if any).
    if (msg instanceof LastHttpContent) {
      for (int i = 0; i < inflightRequests; i++) {
        ctx.writeAndFlush(createPipeliningNotSupported());
      }
    }
    inflightRequests = 0;

    // Recycle the message sender
    if (currentMessageSender != null) {
      Queue<MessageSender> senders = messageSenders.get(currentMessageSender.getDiscoverable());
      if (senders != null) {
        senders.add(currentMessageSender);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    HttpResponse response = cause instanceof HandlerException
      ? ((HandlerException) cause).createFailureResponse()
      : createErrorResponse(cause);
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Finds the {@link Discoverable} for the given {@link HttpRequest} to route to.
   */
  private Discoverable getDiscoverable(HttpRequest httpRequest, InetSocketAddress address) {
    EndpointStrategy strategy = serviceLookup.getDiscoverable(address.getPort(), httpRequest);
    if (strategy == null) {
      throw new HandlerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                 "No endpoint strategy found for request " + getRequestLine(httpRequest));
    }
    Discoverable discoverable = strategy.pick();
    if (discoverable == null) {
      throw new HandlerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                 "No discoverable found for request " + getRequestLine(httpRequest));
    }
    return discoverable;
  }

  /**
   * Returns the {@link MessageSender} for writing messages to the endpoint represented by the given
   * {@link Discoverable}.
   */
  private MessageSender getMessageSender(Channel inboundChannel,
                                         Discoverable discoverable) throws SSLException {
    Queue<MessageSender> senders = messageSenders.get(discoverable);
    if (senders == null) {
      senders = new LinkedList<>();
      messageSenders.put(discoverable, senders);
    }

    // Keeps taking out inactive MessageSender
    MessageSender sender = senders.poll();
    while (sender != null && !sender.isActive()) {
      sender = senders.poll();
    }
    // Found an usable one, return it
    if (sender != null) {
      LOG.trace("Reuse message sender for {}", discoverable);
      return sender;
    }

    // Create new MessageSender by making a new outbound connection.
    ChannelFuture outboundChannelFuture = clientBootstrap.connect(discoverable.getSocketAddress());
    sender = new MessageSender(discoverable, inboundChannel, outboundChannelFuture);
    LOG.trace("Create new message sender for {}", discoverable);
    return sender;
  }

  private String getRequestLine(HttpRequest request) {
    return request.method() + " " + request.uri() + " " + request.protocolVersion();
  }

  private HttpResponse createPipeliningNotSupported() {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_IMPLEMENTED);
    response.content().writeCharSequence("HTTP pipelining is not supported", StandardCharsets.UTF_8);
    HttpUtil.setContentLength(response, response.content().readableBytes());
    return response;
  }

  private HttpResponse createErrorResponse(Throwable cause) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                                                            HttpResponseStatus.INTERNAL_SERVER_ERROR);
    if (cause.getMessage() != null) {
      response.content().writeCharSequence(cause.getMessage(), StandardCharsets.UTF_8);
    }
    HttpUtil.setContentLength(response, response.content().readableBytes());
    return response;
  }


  /**
   * For sending messages to outbound channel while maintaining the order of messages according to
   * the order that {@link #send(Object, ChannelPromise)} method is called.
   *
   * It uses a lock-free algorithm similar to the one
   * in {@link co.cask.cdap.data.stream.service.ConcurrentStreamWriter} to do the write through the
   * channel callback.
   */
  private static final class MessageSender {
    private final Discoverable discoverable;
    private final Channel inboundChannel;
    private final ChannelFuture outboundChannelFuture;
    private final Queue<OutboundMessage> messages;
    private final AtomicBoolean writer;
    private final boolean enableSSL;
    private final AtomicBoolean pipelineUpdated;

    MessageSender(Discoverable discoverable, Channel inboundChannel, ChannelFuture outboundChannelFuture) {
      this.discoverable = discoverable;
      this.inboundChannel = inboundChannel;
      this.outboundChannelFuture = outboundChannelFuture;
      this.messages = Queues.newConcurrentLinkedQueue();
      this.writer = new AtomicBoolean(false);
      this.enableSSL = Arrays.equals(HTTPS_SCHEME_BYTES, discoverable.getPayload());
      this.pipelineUpdated = new AtomicBoolean(false);
    }

    /**
     * Returns {@code true} if the outbound channel is active.
     */
    boolean isActive() {
      return outboundChannelFuture.isSuccess() && outboundChannelFuture.channel().isActive();
    }

    /**
     * Returns the {@link Discoverable} that this sender is associated with.
     */
    Discoverable getDiscoverable() {
      return discoverable;
    }

    void send(Object msg, final ChannelPromise promise) {
      final OutboundMessage message = new OutboundMessage(msg, promise);
      messages.add(message);

      if (outboundChannelFuture.isSuccess()) {
        flushUntilCompleted(outboundChannelFuture.channel(), message);
      } else {
        outboundChannelFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              flushUntilCompleted(future.channel(), message);
            } else {
              // If the outbound future failed, mark the promise as fail
              promise.setFailure(future.cause());
            }
          }
        });
      }
    }

    /**
     * Writes queued messages to the given channel and keep doing it until the given message is written.
     */
    private void flushUntilCompleted(Channel channel, OutboundMessage message) {
      // Retry until the message is sent.
      while (!message.isCompleted()) {
        // If lose to be write, just yield for other threads and recheck if the message is sent.
        if (!writer.compareAndSet(false, true)) {
          Thread.yield();
          continue;
        }

        // Otherwise, send every messages in the queue.
        // The ChannelPromise will be completed when the message is sent (or failed)
        try {
          Throwable failure = null;
          try {
            updateChannelPipeline(channel);
          } catch (Exception e) {
            failure = e;
          }

          OutboundMessage m = messages.poll();
          while (m != null) {
            if (failure == null) {
              m.write(channel);
            } else {
              m.failure(failure);
            }
            m = messages.poll();
          }
        } finally {
          writer.set(false);
        }
      }
    }

    private void updateChannelPipeline(Channel channel) throws SSLException {
      if (pipelineUpdated.compareAndSet(false, true)) {
        // If the pipeline already has the forwarder, which means it's been setup already
        ChannelPipeline pipeline = channel.pipeline();

        if (enableSSL) {
          SslContext sslContext = SslContextBuilder
            .forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE).build();

          pipeline.addFirst("ssl", sslContext.newHandler(channel.alloc()));
          LOG.trace("Adding ssl handler to the pipeline.");
        }

        pipeline.addLast("forwarder", new OutboundHandler(inboundChannel));
      }
    }
  }

  /**
   * A wrapper for a message and the {@link ChannelPromise} to use for writing to a {@link Channel}.
   */
  private static final class OutboundMessage {
    private final Object message;
    private final ChannelPromise promise;

    OutboundMessage(Object message, ChannelPromise promise) {
      this.message = message;
      this.promise = promise;
    }

    boolean isCompleted() {
      return promise.isDone();
    }

    void failure(Throwable cause) {
      promise.setFailure(cause);
    }

    void write(Channel channel) {
      ChannelPromise writePromise = channel.newPromise();
      writePromise.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            promise.setSuccess(future.get());
          } else if (future.isCancelled()) {
            promise.cancel(true);
          } else {
            promise.setFailure(future.cause());
          }
        }
      });

      channel.writeAndFlush(message, writePromise);
    }

    Object getMessage() {
      return message;
    }
  }
}
