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

import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.common.BackupConstants;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;

public class MessageIndexSyncWorker extends AbstractLogSyncWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageIndexSyncWorker.class);

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
            LOGGER.info("reset message log sync offset from {} to {}", originalOffset, startSyncOffset);
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
                    //flag
                    byte flag = body.get();

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

                    control = skipTags(body, flag);

                    if (control == Control.INVALID) {
                        nextSyncOffset = visitor.getStartOffset() + visitor.visitedBufferSize();
                        continue;
                    }
                    if (control == Control.NOSPACE) break;

                    control = writeV2Flag(body, byteBuf);

                    if (control == Control.NOSPACE) break;

                    control = copyPartitionName(body, byteBuf);

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

    private Control writeV2Flag(ByteBuffer body, ByteBuf to) {
        return writeString(BackupConstants.SYNC_V2_FLAG, to);
    }

    private Control copyPartitionName(ByteBuffer body, ByteBuf to) {
        int bodyLen = body.getInt();
        if (bodyLen < 0 || body.remaining() < bodyLen) {
            return Control.INVALID;
        }

        while (body.remaining() > 0) {

            GetPayloadDataResult keyResult = getString(body);
            if (keyResult.control != Control.OK) {
                return keyResult.control;
            }

            String key = keyResult.data;

            if (BaseMessage.keys.qmq_partitionName.name().equals(key)) {
                return copyString(body, to);
            }

        }

        if (!to.isWritable(Short.BYTES + 0)) {
            return Control.NOSPACE;
        }
        PayloadHolderUtils.writeString("", to);;

        return Control.OK;
    }

    private GetPayloadDataResult getString(ByteBuffer body) {
        short len = body.getShort();
        if (len <= 0) {
            return new GetPayloadDataResult(Control.INVALID, null);
        }
        byte[] bs = new byte[len];
        body.get(bs);
        String data = CharsetUtils.toUTF8String(bs);
        return new GetPayloadDataResult(Control.OK, data);
    }

    private static class GetPayloadDataResult {
        private final Control control;
        private final String data;


        private GetPayloadDataResult(Control control, String data) {
            this.control = control;
            this.data = data;
        }
    }

    private Control skipTags(ByteBuffer body, byte flag) {
        if (!Flags.hasTags(flag)) return Control.OK;

        final byte tagsSize = body.get();

        if (tagsSize < 0) {
            return Control.INVALID;
        }

        for (int i = 0; i < tagsSize; i++) {
            int len = body.getShort();
            if (len <= 0) {
                return Control.INVALID;
            }

            try {
                body.position(body.position() + len);
            }
            catch (IllegalArgumentException e) {
                //当body.position() + len > body的最大长度时抛出此异常
                return Control.INVALID;
            }
        }

        return Control.OK;
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

    private Control writeString(String str, ByteBuf to) {
        str = Strings.nullToEmpty(str);

        if (!to.isWritable(Short.BYTES + str.length())) {
            to.resetWriterIndex();
            return Control.NOSPACE;
        }
        PayloadHolderUtils.writeString(str, to);

        return Control.OK;
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
