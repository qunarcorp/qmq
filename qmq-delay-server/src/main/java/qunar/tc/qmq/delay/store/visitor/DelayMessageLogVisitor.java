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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.LogRecordHeader;
import qunar.tc.qmq.delay.store.model.MessageLogRecord;
import qunar.tc.qmq.store.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static qunar.tc.qmq.delay.store.model.MessageLogAttrEnum.ATTR_EMPTY_RECORD;
import static qunar.tc.qmq.delay.store.model.MessageLogAttrEnum.ATTR_SKIP_RECORD;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-11 18:33
 */
public class DelayMessageLogVisitor implements LogVisitor<LogRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayMessageLogVisitor.class);

    public static final EmptyLogRecord EMPTY_LOG_RECORD = new EmptyLogRecord();

    private static final int MIN_RECORD_BYTES = 13;

    private final AtomicInteger visitedBufferSize = new AtomicInteger(0);

    private LogSegment currentSegment;
    private SegmentBuffer currentBuffer;
    private long startOffset;

    public DelayMessageLogVisitor(LogManager logManager, final Long startOffset) {
        long initialOffset = initialOffset(logManager, startOffset);
        this.currentSegment = logManager.locateSegment(initialOffset);
        this.currentBuffer = selectBuffer(initialOffset);
        this.startOffset = initialOffset;
    }

    public long startOffset() {
        return startOffset;
    }

    @Override
    public Optional<LogRecord> nextRecord() {
        if (currentBuffer == null) {
            return Optional.of(EMPTY_LOG_RECORD);
        }

        return readOneRecord(currentBuffer.getBuffer());
    }

    @Override
    public long visitedBufferSize() {
        if (currentBuffer == null) {
            return 0;
        }
        return visitedBufferSize.get();
    }

    private long initialOffset(final LogManager logManager, final Long originStart) {
        long minOffset = logManager.getMinOffset();
        if (originStart < minOffset) {
            LOGGER.error("initial delay message log visitor offset less than min offset. start: {}, min: {}",
                    originStart, minOffset);
            return minOffset;
        }

        return originStart;
    }

    private SegmentBuffer selectBuffer(final long startOffset) {
        if (currentSegment == null) {
            return null;
        }
        final int pos = (int) (startOffset % currentSegment.getFileSize());
        return currentSegment.selectSegmentBuffer(pos);
    }

    private Optional<LogRecord> readOneRecord(final ByteBuffer buffer) {
        if (buffer.remaining() < MIN_RECORD_BYTES) {
            return Optional.of(EMPTY_LOG_RECORD);
        }

        final int startPos = buffer.position();
        long startWroteOffset = startOffset + startPos;
        // magic
        final int magic = buffer.getInt();
        if (!MagicCodeSupport.isValidMessageLogMagicCode(magic)) {
            visitedBufferSize.set(currentBuffer.getSize());
            return Optional.of(EMPTY_LOG_RECORD);
        }

        // attr
        final byte attributes = buffer.get();
        //timestamp
        buffer.getLong();
        if (attributes == ATTR_SKIP_RECORD.getCode()) {
            if (buffer.remaining() < Integer.BYTES) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            // blank size
            final int blankSize = buffer.getInt();
            visitedBufferSize.addAndGet(blankSize + (buffer.position() - startPos));
            return Optional.empty();
        } else if (attributes == ATTR_EMPTY_RECORD.getCode()) {
            visitedBufferSize.set(currentBuffer.getSize());
            return Optional.of(EMPTY_LOG_RECORD);
        } else {
            if (buffer.remaining() < Long.BYTES) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            // schedule time
            long scheduleTime = buffer.getLong();
            if (buffer.remaining() < Long.BYTES) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            // logical offset
            long sequence = buffer.getLong();
            if (buffer.remaining() < Integer.BYTES) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            // message id size
            final int messageIdSize = buffer.getInt();
            if (buffer.remaining() < messageIdSize) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            final byte[] messageIdBytes = new byte[messageIdSize];
            // message id
            buffer.get(messageIdBytes);

            // subject size
            final int subjectSize = buffer.getInt();
            if (buffer.remaining() < subjectSize) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            final byte[] subjectBytes = new byte[subjectSize];
            // subject
            buffer.get(subjectBytes);

            if (magic >= MagicCode.MESSAGE_LOG_MAGIC_V2) {
                if (buffer.remaining() < Long.BYTES) {
                    return Optional.of(EMPTY_LOG_RECORD);
                }
                // crc
                buffer.getLong();
            }

            if (buffer.remaining() < Integer.BYTES) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            // payload size
            final int payloadSize = buffer.getInt();
            if (buffer.remaining() < payloadSize) {
                return Optional.of(EMPTY_LOG_RECORD);
            }
            // message body && The new buffer's position will be zero
            final ByteBuffer message = buffer.slice();
            message.limit(payloadSize);
            buffer.position(buffer.position() + payloadSize);
            int recordBytes = buffer.position() - startPos;
            visitedBufferSize.addAndGet(recordBytes);
            LogRecordHeader header = new LogRecordHeader(new String(subjectBytes, StandardCharsets.UTF_8), new String(messageIdBytes, StandardCharsets.UTF_8), scheduleTime, sequence);
            return Optional.of(new MessageLogRecord(header, recordBytes, startWroteOffset, payloadSize, message));
        }
    }

    @Override
    public void close() {

    }

    public static class EmptyLogRecord implements LogRecord {

        @Override
        public String getSubject() {
            return null;
        }

        @Override
        public String getMessageId() {
            return null;
        }

        @Override
        public long getScheduleTime() {
            return 0;
        }

        @Override
        public int getPayloadSize() {
            return 0;
        }

        @Override
        public ByteBuffer getRecord() {
            return null;
        }

        @Override
        public long getStartWroteOffset() {
            return 0;
        }

        @Override
        public int getRecordSize() {
            return 0;
        }

        @Override
        public long getSequence() {
            return 0;
        }
    }
}
