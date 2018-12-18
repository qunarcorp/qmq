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

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.store.SegmentBuffer;
import qunar.tc.qmq.sync.DelaySyncRequest;
import qunar.tc.qmq.util.RemotingBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class MessageLogSyncWorker extends AbstractLogSyncWorker implements Disposable {
    private final AsyncEventBus messageLogSyncEventBus;
    private final ExecutorService dispatchExecutor;

    public MessageLogSyncWorker(DelayLogFacade delayLogFacade, DynamicConfig config) {
        super(delayLogFacade, config);
        this.dispatchExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("heart-event-bus-%d").build());
        this.messageLogSyncEventBus = new AsyncEventBus(dispatchExecutor);
    }

    @Override
    public void newTimeout(DelaySyncRequestProcessor.SyncRequestTimeoutTask task) {
        final long timeout = config.getLong("message.sync.timeout.ms", 10L);
        processorTimer.schedule(task, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public SegmentBuffer getSyncLog(DelaySyncRequest syncRequest) {
        messageLogSyncEventBus.post(syncRequest);
        long startSyncOffset = syncRequest.getMessageLogOffset();
        if (startSyncOffset <= 0) {
            startSyncOffset = delayLogFacade.getMessageLogMinOffset();
        }

        return delayLogFacade.getMessageLogs(startSyncOffset);
    }

    @Override
    protected void processSyncLog(DelaySyncRequestProcessor.SyncRequestEntry entry, SegmentBuffer result) {
        final int batchSize = config.getInt("sync.batch.size", 100000);
        final long startOffset = result.getStartOffset();
        final ByteBuffer buffer = result.getBuffer();
        int size = result.getSize();

        if (size > batchSize) {
            buffer.limit(batchSize);
            size = batchSize;
        }
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, entry.getRequestHeader()
                , new SyncLogPayloadHolder(size, startOffset, buffer));
        entry.getCtx().writeAndFlush(datagram);
    }

    @Override
    protected Datagram resolveResult(DelaySyncRequest syncRequest, RemotingHeader header) {
        long offset = syncRequest.getMessageLogOffset();
        SyncLogPayloadHolder payloadHolder = new SyncLogPayloadHolder(0, offset, null);
        return RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, header, payloadHolder);
    }

    void registerSyncEvent(Object listener) {
        messageLogSyncEventBus.register(listener);
    }

    @Override
    public void destroy() {
        if (dispatchExecutor != null) {
            dispatchExecutor.shutdown();
        }
    }

    private static class SyncLogPayloadHolder implements PayloadHolder {
        private final int size;
        private final long startOffset;
        private final ByteBuffer buffer;

        SyncLogPayloadHolder(int size, long startOffset, ByteBuffer buffer) {
            this.size = size;
            this.startOffset = startOffset;
            this.buffer = buffer;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeInt(size);
            out.writeLong(startOffset);
            if (buffer != null) {
                out.writeBytes(buffer);
            }
        }
    }
}
