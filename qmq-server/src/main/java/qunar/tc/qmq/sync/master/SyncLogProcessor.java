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

package qunar.tc.qmq.sync.master;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.sync.SyncType;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
class SyncLogProcessor implements NettyRequestProcessor, Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(SyncLogProcessor.class);

    private final ExecutorService executor;
    private final MessageLogSyncWorker messageLogSyncWorker;
    private final Map<Integer, SyncProcessor> processorMap;

    SyncLogProcessor(Storage storage, DynamicConfig config) {
        this.executor = Executors.newFixedThreadPool(3, new NamedThreadFactory("master-sync"));
        this.processorMap = new HashMap<>();
        this.messageLogSyncWorker = new MessageLogSyncWorker(storage, config);
        final ActionLogSyncWorker actionLogSyncWorker = new ActionLogSyncWorker(storage, config);
        final HeartbeatSyncWorker heartBeatSyncWorker = new HeartbeatSyncWorker(storage);
        processorMap.put(SyncType.message.getCode(), messageLogSyncWorker);
        processorMap.put(SyncType.action.getCode(), actionLogSyncWorker);
        processorMap.put(SyncType.heartbeat.getCode(), heartBeatSyncWorker);
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        final SyncRequest syncRequest = deserializeSyncRequest(request);
        final int syncType = syncRequest.getSyncType();
        final SyncProcessor processor = processorMap.get(syncType);
        if (processor == null) {
            LOG.error("unknown sync type {}", syncType);
            final Datagram datagram = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.BROKER_ERROR, request.getHeader());
            return CompletableFuture.completedFuture(datagram);
        }

        final SyncRequestEntry entry = new SyncRequestEntry(ctx, request.getHeader(), syncRequest);
        executor.submit(new SyncRequestProcessTask(entry, processor));
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    void registerSyncEvent(Object listener) {
        this.messageLogSyncWorker.registerSyncEvent(listener);
    }

    private SyncRequest deserializeSyncRequest(final RemotingCommand request) {
        final ByteBuf body = request.getBody();
        final int logType = body.readByte();
        final long messageLogOffset = body.readLong();
        final long actionLogOffset = body.readLong();
        return new SyncRequest(logType, messageLogOffset, actionLogOffset);
    }

    @Override
    public void destroy() {
        messageLogSyncWorker.destroy();
        executor.shutdown();
    }

    private static class SyncRequestProcessTask implements Runnable {
        private final SyncRequestEntry entry;
        private final SyncProcessor processor;

        private SyncRequestProcessTask(SyncRequestEntry entry, SyncProcessor processor) {
            this.entry = entry;
            this.processor = processor;
        }

        @Override
        public void run() {
            try {
                processor.process(entry);
            } catch (Exception e) {
                LOG.error("process sync request error", e);
            }
        }
    }
}
