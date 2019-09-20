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
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;

public class MessageIndexSyncWorker extends AbstractLogSyncWorker {
    private static final Logger LOG = LoggerFactory.getLogger(MessageIndexSyncWorker.class);

    private static final int MAX_SYNC_NUM = 5000;

    private final int batchSize;
    private final Storage storage;

    MessageIndexSyncWorker(Storage storage, DynamicConfig config) {
        super(config);
        this.storage = storage;
        this.batchSize = config.getInt("sync.batch.size", 100000);
    }

    @Override
    protected SegmentBuffer getSyncLog(SyncRequest syncRequest) {
        final long originalOffset = syncRequest.getMessageLogOffset();
        long startSyncOffset = originalOffset;

        long minMessageOffset = storage.getMinMessageOffset();
        if (startSyncOffset < minMessageOffset) {
            startSyncOffset = minMessageOffset;
            LOG.info("reset message log sync offset from {} to {}", originalOffset, startSyncOffset);
        }

        try (MessageLogRecordVisitor visitor = storage.newMessageLogVisitor(startSyncOffset)) {
            LogSegment currentSegment = null;
            ByteBuf byteBuf = ByteBufAllocator.DEFAULT.ioBuffer(batchSize);
            long nextSyncOffset = originalOffset;
            try {
                for (int i = 0; i < MAX_SYNC_NUM; ++i) {
                    LogVisitorRecord<MessageLogRecord> record = visitor.nextRecord();

                    if (record.isNoMore()) {
                        nextSyncOffset = visitor.getStartOffset() + visitor.visitedBufferSize();
                        break;
                    }
                    if (!record.hasData()) {
                        nextSyncOffset = visitor.getStartOffset() + visitor.visitedBufferSize();
                        continue;
                    }

                    MessageLogRecord data = record.getData();
                    currentSegment = data.getLogSegment();

                    byteBuf.markWriterIndex();

                    //sequence
                    if (!writeLong(data.getSequence(), byteBuf)) break;

                    ByteBuffer body = data.getPayload();
                    //skip flag
                    body.get();

                    //create time
                    if (!writeLong(body.getLong(), byteBuf)) break;

                    //skip expireTime
                    body.getLong();

                    //subject
                    Control control = copyString(body, byteBuf);
                    if (control == Control.INVALID) {
                        nextSyncOffset = visitor.getStartOffset() + visitor.visitedBufferSize();
                        continue;
                    }
                    if (control == Control.NOSPACE) break;

                    //message id
                    control = copyString(body, byteBuf);
                    if (control == Control.INVALID) {
                        nextSyncOffset = visitor.getStartOffset() + visitor.visitedBufferSize();
                        continue;
                    }
                    if (control == Control.NOSPACE) break;

                    nextSyncOffset = visitor.getStartOffset() + visitor.visitedBufferSize();
                }

            } finally {
                if (!byteBuf.isReadable()) {
                    byteBuf.release();
                }
            }

            if (originalOffset == nextSyncOffset) {
                return null;
            }

            //FIXME: 这里完全是为了避免父类里做超时处理
            if (byteBuf.isReadable()) {
                return new ByteBufSegmentBuffer(nextSyncOffset, currentSegment, byteBuf, byteBuf.readableBytes());
            } else {
                return new ByteBufSegmentBuffer(nextSyncOffset);
            }
        }
    }

    private boolean writeLong(long value, ByteBuf to) {
        if (!to.isWritable(Long.BYTES)) {
            to.resetWriterIndex();
            return false;
        }
        to.writeLong(value);
        return true;
    }

    private enum Control {
        NOSPACE,
        INVALID,
        OK
    }

    private Control copyString(ByteBuffer from, ByteBuf to) {
        short len = from.getShort();
        if (len <= 0) {
            to.resetWriterIndex();
            return Control.INVALID;
        }
        byte[] str = new byte[len];
        from.get(str);
        if (!to.isWritable(Short.BYTES + len)) {
            to.resetWriterIndex();
            return Control.NOSPACE;
        }
        PayloadHolderUtils.writeString(str, to);
        return Control.OK;
    }

    private static class ByteBufSegmentBuffer extends SegmentBuffer {
        private final ByteBuf byteBuf;

        ByteBufSegmentBuffer(long startOffset) {
            super(startOffset, null, 0, null);
            this.byteBuf = null;
        }

        ByteBufSegmentBuffer(long startOffset, LogSegment segment, ByteBuf buffer, int size) {
            super(startOffset, buffer.nioBuffer(), size, segment);
            this.byteBuf = buffer;
        }

        @Override
        public boolean retain() {
            if (getLogSegment() == null) return false;

            boolean retain = super.retain();
            if (!retain) {
                byteBuf.release();
            }
            return retain;
        }

        @Override
        public boolean release() {
            if (getLogSegment() == null) return false;

            byteBuf.release();
            return super.release();
        }
    }
}
