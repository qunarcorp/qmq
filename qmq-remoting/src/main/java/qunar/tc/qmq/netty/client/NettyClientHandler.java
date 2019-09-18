/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.netty.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.util.RemoteHelper;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yiqun.fan create on 17-8-29.
 */
@ChannelHandler.Sharable
class NettyClientHandler extends SimpleChannelInboundHandler<Datagram> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClientHandler.class);

    private static final long CLEAN_RESPONSE_TABLE_PERIOD_MILLIS = 1000;

    private final AtomicInteger opaque = new AtomicInteger(0);
    private final ConcurrentMap<Channel, ConcurrentMap<Integer, ResponseFuture>> requestsInFlight = new ConcurrentHashMap<>(4);
    private final ScheduledExecutorService timeoutTracker;

    NettyClientHandler() {
        timeoutTracker = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-clean"));
        timeoutTracker.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NettyClientHandler.this.processTimeouts();
            }
        }, 3 * CLEAN_RESPONSE_TABLE_PERIOD_MILLIS, CLEAN_RESPONSE_TABLE_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
    }

    ResponseFuture newResponse(Channel channel, long timeoutMills, ResponseFuture.Callback callback) throws ClientSendException {
        final int op = opaque.getAndIncrement();
        ResponseFuture future = new ResponseFuture(op, timeoutMills, callback);
        ConcurrentMap<Integer, ResponseFuture> channelBuffer = requestsInFlight.get(channel);
        if (channelBuffer == null) {
            channelBuffer = new ConcurrentHashMap<>();
            ConcurrentMap<Integer, ResponseFuture> old = requestsInFlight.putIfAbsent(channel, channelBuffer);
            if (old != null) {
                channelBuffer = old;
            }
        }

        if (channelBuffer.putIfAbsent(op, future) != null) {
            throw new ClientSendException(ClientSendException.SendErrorCode.ILLEGAL_OPAQUE);
        }
        return future;
    }

    void removeResponse(Channel channel, ResponseFuture responseFuture) {
        ConcurrentMap<Integer, ResponseFuture> channelBuffer = requestsInFlight.get(channel);
        if (channelBuffer == null) return;

        channelBuffer.remove(responseFuture.getOpaque(), responseFuture);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Datagram datagram) {
        if (datagram == null) return;

        try {
            processResponse(ctx, datagram);
        } catch (Exception e) {
            LOGGER.error("processResponse exception", e);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ConcurrentMap<Integer, ResponseFuture> channelBuffer = requestsInFlight.remove(ctx.channel());
        if (channelBuffer == null) return;

        for (Map.Entry<Integer, ResponseFuture> entry : channelBuffer.entrySet()) {
            ResponseFuture responseFuture = entry.getValue();
            responseFuture.completeByTimeoutClean();
        }
    }

    private void processResponse(ChannelHandlerContext ctx, Datagram response) {
        int opaque = response.getHeader().getOpaque();
        ConcurrentMap<Integer, ResponseFuture> channelBuffer = requestsInFlight.get(ctx.channel());
        if (channelBuffer == null) return;

        ResponseFuture responseFuture = channelBuffer.remove(opaque);
        if (responseFuture != null) {
            responseFuture.completeByReceiveResponse(response);
        } else {
            LOGGER.warn("receive response, but not matched any request, maybe response timeout or channel had been closed, {}", RemoteHelper.parseChannelRemoteAddress(ctx.channel()));
        }
    }

    private void processTimeouts() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Map.Entry<Channel, ConcurrentMap<Integer, ResponseFuture>>> channelBuffers = this.requestsInFlight.entrySet().iterator();
        while (channelBuffers.hasNext()) {
            Map.Entry<Channel, ConcurrentMap<Integer, ResponseFuture>> channelBuffer = channelBuffers.next();
            Iterator<Map.Entry<Integer, ResponseFuture>> iterator = channelBuffer.getValue().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, ResponseFuture> next = iterator.next();
                ResponseFuture future = next.getValue();

                if (isTimeout(future)) {
                    iterator.remove();

                    rfList.add(future);
                    LOGGER.warn("remove timeout request, " + future);
                }

            }
        }

        completeFutures(rfList);
    }

    private boolean isTimeout(ResponseFuture future) {
        return future.getTimeout() >= 0 && (future.getBeginTime() + future.getTimeout()) <= System.currentTimeMillis();
    }

    private void completeFutures(List<ResponseFuture> rfList) {
        for (ResponseFuture responseFuture : rfList) {
            try {
                responseFuture.completeByTimeoutClean();
            } catch (Throwable e) {
                LOGGER.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    void shutdown() {
        timeoutTracker.shutdown();
    }
}
