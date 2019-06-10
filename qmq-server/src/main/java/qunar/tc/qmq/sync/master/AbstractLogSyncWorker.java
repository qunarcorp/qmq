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
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.store.DataTransfer;
import qunar.tc.qmq.store.LogSegment;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.sync.SyncType;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.HeaderSerializer;
import qunar.tc.qmq.utils.ServerTimerUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/21
 */
abstract class AbstractLogSyncWorker implements SyncProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractLogSyncWorker.class);

    private static final int SYNC_HEADER_LEN = 4 /*size*/ + 8 /*startOffset*/;

    private final DynamicConfig config;
    private volatile LogSegment currentSegment;

    AbstractLogSyncWorker(final DynamicConfig config) {
        this.config = config;
        this.currentSegment = null;
    }

    @Override
    public void process(SyncRequestEntry entry) {
        final SyncRequest syncRequest = entry.getSyncRequest();
        final SegmentBuffer result = getSyncLog(syncRequest);
        if (result == null || result.getSize() <= 0) {
            final long timeout = config.getLong("message.sync.timeout.ms", 10L);
            ServerTimerUtil.newTimeout(new SyncRequestTimeoutTask(entry, this), timeout, TimeUnit.MILLISECONDS);
            return;
        }

        processSyncLog(entry, result);
    }

    @Override
    public void processTimeout(SyncRequestEntry entry) {
        final SyncRequest syncRequest = entry.getSyncRequest();
        final SegmentBuffer result = getSyncLog(syncRequest);

        int syncType = syncRequest.getSyncType();
        if (result == null || result.getSize() <= 0) {
            long offset = syncType == SyncType.message.getCode() ? syncRequest.getMessageLogOffset() : syncRequest.getActionLogOffset();
            writeEmpty(entry, offset);
            return;
        }

        processSyncLog(entry, result);
    }

    protected abstract SegmentBuffer getSyncLog(SyncRequest syncRequest);

    private void writeEmpty(SyncRequestEntry entry, long offset) {
        final SyncLogPayloadHolder payloadHolder = new SyncLogPayloadHolder(offset);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, entry.getRequestHeader(), payloadHolder);
        entry.getCtx().writeAndFlush(datagram);
    }

    private void processSyncLog(SyncRequestEntry entry, SegmentBuffer result) {
        if (!acquireSegmentLock(result)) {
            writeEmpty(entry, result.getStartOffset());
            return;
        }

        if (!result.retain()) {
            writeEmpty(entry, result.getStartOffset());
            return;
        }
        try {
            final int batchSize = config.getInt("sync.batch.size", 100000);
            final ByteBuffer buffer = result.getBuffer();
            int size = result.getSize();
            if (size > batchSize) {
                buffer.limit(batchSize);
                size = batchSize;
            }
            final RemotingHeader header = RemotingBuilder.buildResponseHeader(CommandCode.SUCCESS, entry.getRequestHeader());
            ByteBuf headerBuffer = HeaderSerializer.serialize(header, SYNC_HEADER_LEN + size, SYNC_HEADER_LEN);
            headerBuffer.writeInt(size);
            headerBuffer.writeLong(result.getStartOffset());
            entry.getCtx().writeAndFlush(new DataTransfer(headerBuffer, result, size));
        } catch (Exception e) {
            result.release();
        }
    }

    private boolean acquireSegmentLock(final SegmentBuffer result) {
        if (BrokerConfig.getBrokerRole() != BrokerRole.MASTER) {
            return true;
        }

        final LogSegment segment = result.getLogSegment();
        if (segment == null) {
            return true;
        }

        if (currentSegment != null && currentSegment.getBaseOffset() == segment.getBaseOffset()) {
            return true;
        }

        if (currentSegment != null) {
            final boolean release = currentSegment.release();
            LOG.info("release segment lock for {} while sync log, release result: {}", currentSegment, release);
            currentSegment = null;
        }

        final boolean retain = segment.retain();
        LOG.info("acquire segment lock for {} while sync log, retain result: {}", segment, retain);
        if (retain) {
            currentSegment = segment;
        }
        return retain;
    }

    private static class SyncLogPayloadHolder implements PayloadHolder {
        private final long startOffset;

        SyncLogPayloadHolder(long startOffset) {
            this.startOffset = startOffset;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeInt(0);
            out.writeLong(startOffset);
        }
    }

    static class SyncRequestTimeoutTask implements TimerTask {
        private final SyncRequestEntry entry;
        private final SyncProcessor processor;

        SyncRequestTimeoutTask(SyncRequestEntry entry, SyncProcessor processor) {
            this.entry = entry;
            this.processor = processor;
        }

        @Override
        public void run(Timeout timeout) {
            try {
                processor.processTimeout(entry);
            } catch (Exception e) {
                LOG.error("process sync request error", e);
            }
        }
    }
}
