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

package qunar.tc.qmq.delay.sync.master;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.sync.DelaySyncRequest;
import qunar.tc.qmq.sync.SyncType;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-10 11:33
 */
public class DelaySyncRequestProcessor implements NettyRequestProcessor, Disposable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelaySyncRequestProcessor.class);

    private final ExecutorService syncProcessPool;
    private final Map<Integer, SyncProcessor> processorMap;
    private final MessageLogSyncWorker messageLogSyncWorker;
    private final ScheduledExecutorService processorTimer;

    DelaySyncRequestProcessor(int scale, DelayLogFacade delayLogFacade, DynamicConfig config) {
        this.processorMap = new HashMap<>();
        this.messageLogSyncWorker = new MessageLogSyncWorker(delayLogFacade, config);
        final DispatchLogSyncWorker dispatchLogSyncWorker = new DispatchLogSyncWorker(scale, delayLogFacade, config);
        final HeartbeatSyncWorker heartbeatSyncWorker = new HeartbeatSyncWorker(delayLogFacade);

        this.processorMap.put(SyncType.message.getCode(), this.messageLogSyncWorker);
        this.processorMap.put(SyncType.dispatch.getCode(), dispatchLogSyncWorker);
        this.processorMap.put(SyncType.heartbeat.getCode(), heartbeatSyncWorker);

        int workerSize = processorMap.size();
        this.syncProcessPool = Executors.newFixedThreadPool(workerSize,
                new ThreadFactoryBuilder().setNameFormat("delay-master-sync-%d").build());
        this.processorTimer = Executors.newScheduledThreadPool(workerSize - 1,
                new ThreadFactoryBuilder().setNameFormat("delay-processor-timer-%d").build());

        this.messageLogSyncWorker.defineTimer(processorTimer);
        dispatchLogSyncWorker.defineTimer(processorTimer);
    }

    @Override
    public void destroy() {
        syncProcessPool.shutdown();
        if (null != processorTimer) {
            processorTimer.shutdown();
            try {
                processorTimer.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Shutdown processorTimer interrupted.");
            }
        }
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        DelaySyncRequest delaySyncRequest = deserializeSyncRequest(request);
        int logType = delaySyncRequest.getSyncType();
        SyncProcessor processor = processorMap.get(logType);
        if (null == processor) {
            LOGGER.error("unknown log type {}", logType);
            final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.BROKER_ERROR, request.getHeader(), null);
            return CompletableFuture.completedFuture(datagram);
        }

        SyncRequestEntry entry = new SyncRequestEntry(ctx, request.getHeader(), delaySyncRequest);
        syncProcessPool.submit(new SyncRequestProcessTask(entry, processor));
        return null;
    }

    private DelaySyncRequest deserializeSyncRequest(RemotingCommand request) {
        ByteBuf body = request.getBody();
        int logType = body.readByte();
        long messageLogOffset = body.readLong();
        int dispatchLogSegmentBaseOffset = body.readInt();
        long dispatchLogOffset = body.readLong();
        int lastDispatchLogBaseOffset = body.readInt();
        long lastDispatchLogOffset = body.readLong();
        return new DelaySyncRequest(messageLogOffset, dispatchLogSegmentBaseOffset, dispatchLogOffset, lastDispatchLogBaseOffset, lastDispatchLogOffset, logType);
    }

    void registerSyncEvent(Object listener) {
        this.messageLogSyncWorker.registerSyncEvent(listener);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    interface SyncProcessor {
        void process(SyncRequestEntry entry);

        void processTimeout(SyncRequestEntry entry);
    }

    static class SyncRequestEntry {
        private final ChannelHandlerContext ctx;
        private final RemotingHeader requestHeader;
        private final DelaySyncRequest delaySyncRequest;

        SyncRequestEntry(ChannelHandlerContext ctx, RemotingHeader requestHeader, DelaySyncRequest delaySyncRequest) {
            this.ctx = ctx;
            this.requestHeader = requestHeader;
            this.delaySyncRequest = delaySyncRequest;
        }

        ChannelHandlerContext getCtx() {
            return ctx;
        }

        RemotingHeader getRequestHeader() {
            return requestHeader;
        }

        DelaySyncRequest getDelaySyncRequest() {
            return delaySyncRequest;
        }
    }

    private static class SyncRequestProcessTask implements Runnable {
        private final SyncRequestEntry entry;
        private final SyncProcessor processor;

        private SyncRequestProcessTask(SyncRequestEntry syncRequestEntry, SyncProcessor processor) {
            this.entry = syncRequestEntry;
            this.processor = processor;
        }

        @Override
        public void run() {
            try {
                processor.process(entry);
            } catch (Exception e) {
                LOGGER.error("process sync request error", e);
            }
        }
    }

    static class SyncRequestTimeoutTask implements Runnable {
        private final SyncRequestEntry entry;
        private final SyncProcessor processor;

        SyncRequestTimeoutTask(SyncRequestEntry entry, SyncProcessor processor) {
            this.entry = entry;
            this.processor = processor;
        }

        @Override
        public void run() {
            try {
                processor.processTimeout(entry);
            } catch (Exception e) {
                LOGGER.error("process sync timeout request error", e);
            }
        }
    }
}
