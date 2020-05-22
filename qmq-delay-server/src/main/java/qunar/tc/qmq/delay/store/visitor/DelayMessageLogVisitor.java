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

package qunar.tc.qmq.delay.store.visitor;

import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.LogRecordHeader;
import qunar.tc.qmq.delay.store.model.MessageLogRecord;
import qunar.tc.qmq.store.AbstractLogVisitor;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.store.buffer.SegmentBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static qunar.tc.qmq.delay.store.model.MessageLogAttrEnum.ATTR_EMPTY_RECORD;
import static qunar.tc.qmq.delay.store.model.MessageLogAttrEnum.ATTR_SKIP_RECORD;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-11 18:33
 */
public class DelayMessageLogVisitor extends AbstractLogVisitor<LogRecord> {

    private static final int MIN_RECORD_BYTES = 13;

    public DelayMessageLogVisitor(LogManager logManager, final long startOffset) {
        super(logManager, startOffset);
    }

    @Override
    protected LogVisitorRecord<LogRecord> readOneRecord(SegmentBuffer segmentBuffer) {
        ByteBuffer buffer = segmentBuffer.getBuffer();
        if (buffer.remaining() < MIN_RECORD_BYTES) {
            return LogVisitorRecord.noMore();
        }

        final int startPos = buffer.position();
        final long wroteOffset = getStartOffset() + startPos;

        // magic
        final int magic = buffer.getInt();
        if (!MagicCodeSupport.isValidMessageLogMagicCode(magic)) {
            setVisitedBufferSize(getBufferSize());
            return LogVisitorRecord.noMore();
        }

        // attr
        final byte attributes = buffer.get();
        //timestamp
        buffer.getLong();
        if (attributes == ATTR_SKIP_RECORD.getCode()) {
            if (buffer.remaining() < Integer.BYTES) {
                return LogVisitorRecord.noMore();
            }
            // blank size
            final int blankSize = buffer.getInt();
            incrVisitedBufferSize(blankSize + (buffer.position() - startPos));
            return LogVisitorRecord.empty();
        } else if (attributes == ATTR_EMPTY_RECORD.getCode()) {
            setVisitedBufferSize(getBufferSize());
            return LogVisitorRecord.noMore();
        } else {
            // schedule time
            if (buffer.remaining() < Long.BYTES) {
                return LogVisitorRecord.noMore();
            }
            long scheduleTime = buffer.getLong();

            // sequence
            if (buffer.remaining() < Long.BYTES) {
                return LogVisitorRecord.noMore();
            }
            long sequence = buffer.getLong();

            // message id size
            if (buffer.remaining() < Integer.BYTES) {
                return LogVisitorRecord.noMore();
            }
            final int messageIdSize = buffer.getInt();

            // message id
            if (buffer.remaining() < messageIdSize) {
                return LogVisitorRecord.noMore();
            }
            final byte[] messageIdBytes = new byte[messageIdSize];
            buffer.get(messageIdBytes);

            // subject size
            if (buffer.remaining() < Integer.BYTES) {
                return LogVisitorRecord.noMore();
            }
            final int subjectSize = buffer.getInt();

            // subject
            if (buffer.remaining() < subjectSize) {
                return LogVisitorRecord.noMore();
            }
            final byte[] subjectBytes = new byte[subjectSize];
            buffer.get(subjectBytes);

            if (magic >= MagicCode.MESSAGE_LOG_MAGIC_V2) {
                if (buffer.remaining() < Long.BYTES) {
                    return LogVisitorRecord.noMore();
                }
                // crc
                buffer.getLong();
            }

            // payload size
            if (buffer.remaining() < Integer.BYTES) {
                return LogVisitorRecord.noMore();
            }
            final int payloadSize = buffer.getInt();

            // message body && The new buffer's position will be zero
            if (buffer.remaining() < payloadSize) {
                return LogVisitorRecord.noMore();
            }
            final ByteBuffer message = buffer.slice();
            message.limit(payloadSize);

            buffer.position(buffer.position() + payloadSize);
            int recordBytes = buffer.position() - startPos;
            incrVisitedBufferSize(recordBytes);

            String subject = new String(subjectBytes, StandardCharsets.UTF_8);
            String messageId = new String(messageIdBytes, StandardCharsets.UTF_8);
            LogRecordHeader header = new LogRecordHeader(subject, messageId, scheduleTime, sequence);
            return LogVisitorRecord.data(new MessageLogRecord(header, recordBytes, wroteOffset, payloadSize, message));
        }
    }

}
