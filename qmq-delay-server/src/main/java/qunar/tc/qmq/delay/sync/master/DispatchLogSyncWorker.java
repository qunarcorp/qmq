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

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.base.SegmentBufferExtend;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.store.SegmentBuffer;
import qunar.tc.qmq.sync.DelaySyncRequest;
import qunar.tc.qmq.util.RemotingBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.delay.store.log.ScheduleOffsetResolver.resolveSegment;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-10 16:13
 */
public class DispatchLogSyncWorker extends AbstractLogSyncWorker {

    private static final long GREENWICH_MEAN_FIRST_YEAR = 197001010801L;
    private final int segmentScale;

    DispatchLogSyncWorker(int scale, DelayLogFacade delayLogFacade, DynamicConfig config) {
        super(delayLogFacade, config);
        this.segmentScale = scale;
    }

    @Override
    protected void newTimeout(DelaySyncRequestProcessor.SyncRequestTimeoutTask task) {
        final long timeout = config.getLong("dispatch.sync.timeout.ms", 10L);
        processorTimer.schedule(task, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    protected SegmentBuffer getSyncLog(DelaySyncRequest delaySyncRequest) {
        long nowSegment = resolveSegment(System.currentTimeMillis(), segmentScale);

        SyncOffset syncOffset = resolveOffset(delaySyncRequest);
        long segmentBaseOffset = syncOffset.getBaseOffset();
        if (syncOffset.getBaseOffset() == -1) return null;

        long maxDispatchLogOffset = delayLogFacade.getDispatchLogMaxOffset(segmentBaseOffset);
        long dispatchLogOffset = syncOffset.getOffset();
        if (dispatchLogOffset < 0) {
            LOGGER.warn("dispatch sync illegal param, baseOffset {} dispatchLogOffset {}", segmentBaseOffset, dispatchLogOffset);
            dispatchLogOffset = 0;
        }

        if (segmentBaseOffset < nowSegment && dispatchLogOffset >= maxDispatchLogOffset) {
            if (maxDispatchLogOffset == 0) return null;

            long nextSegmentBaseOffset = delayLogFacade.higherDispatchLogBaseOffset(segmentBaseOffset);
            if (nextSegmentBaseOffset < 0) {
                return null;
            }

            // sync next
            segmentBaseOffset = nextSegmentBaseOffset;
            dispatchLogOffset = 0;
        }

        SegmentBuffer result = delayLogFacade.getDispatchLogs(segmentBaseOffset, dispatchLogOffset);

        // previous segment(< now) size may be 0, then skip, wait until timeout
        while (result != null && result.getSize() <= 0) {
            segmentBaseOffset = delayLogFacade.higherDispatchLogBaseOffset(segmentBaseOffset);
            if (segmentBaseOffset < 0 || segmentBaseOffset > nowSegment) {
                return null;
            }
            result = delayLogFacade.getDispatchLogs(segmentBaseOffset, dispatchLogOffset);
        }

        return result;
    }

    private SyncOffset resolveOffset(final DelaySyncRequest delaySyncRequest) {
        long segmentBaseOffset = delaySyncRequest.getDispatchSegmentBaseOffset();
        long offset = delaySyncRequest.getDispatchLogOffset();
        long lastSegmentBaseOffset = delaySyncRequest.getLastDispatchSegmentBaseOffset();
        long lastOffset = delaySyncRequest.getLastDispatchSegmentOffset();

        // sync the first time
        if (segmentBaseOffset < GREENWICH_MEAN_FIRST_YEAR) {
            return new SyncOffset(delayLogFacade.higherDispatchLogBaseOffset(segmentBaseOffset), 0);
        }

        // only one dispatch segment
        if (lastSegmentBaseOffset < GREENWICH_MEAN_FIRST_YEAR) {
            return new SyncOffset(segmentBaseOffset, offset);
        }

        long lastSegmentMaxOffset = delayLogFacade.getDispatchLogMaxOffset(lastSegmentBaseOffset);
        // sync last
        if (lastOffset < lastSegmentMaxOffset) {
            return new SyncOffset(lastSegmentBaseOffset, lastOffset);
        }

        // sync current, probably
        return new SyncOffset(segmentBaseOffset, offset);
    }

    @Override
    protected void processSyncLog(DelaySyncRequestProcessor.SyncRequestEntry entry, SegmentBuffer result) {
        int batchSize = config.getInt("sync.batch.size", 100000);
        long startOffset = result.getStartOffset();
        ByteBuffer buffer = result.getBuffer();
        int size = result.getSize();
        long segmentBaseOffset = ((SegmentBufferExtend) result).getBaseOffset();

        if (size > batchSize) {
            buffer.limit(batchSize);
            size = batchSize;
        }
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, entry.getRequestHeader()
                , new SyncDispatchLogPayloadHolder(size, startOffset, segmentBaseOffset, buffer));
        entry.getCtx().writeAndFlush(datagram);
    }

    @Override
    protected Datagram resolveResult(DelaySyncRequest syncRequest, RemotingHeader header) {
        long offset = syncRequest.getDispatchLogOffset();
        SyncDispatchLogPayloadHolder payloadHolder = new SyncDispatchLogPayloadHolder(0, offset, syncRequest.getDispatchSegmentBaseOffset(), null);
        return RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, header, payloadHolder);
    }

    public static class SyncDispatchLogPayloadHolder implements PayloadHolder {
        private final int size;
        private final long startOffset;
        private final long segmentBaseOffset;
        private final ByteBuffer buffer;

        SyncDispatchLogPayloadHolder(int size, long startOffset, long segmentBaseOffset, ByteBuffer buffer) {
            this.size = size;
            this.startOffset = startOffset;
            this.segmentBaseOffset = segmentBaseOffset;
            this.buffer = buffer;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeInt(size);
            out.writeLong(startOffset);
            out.writeLong(segmentBaseOffset);
            if (buffer != null) {
                out.writeBytes(buffer);
            }
        }
    }

    private static class SyncOffset {
        private final long baseOffset;
        private final long offset;

        SyncOffset(long baseOffset, long offset) {
            this.baseOffset = baseOffset;
            this.offset = offset;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public long getOffset() {
            return offset;
        }
    }
}
