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

package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.buffer.SegmentBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author keli.wang
 * @since 2017/7/5
 */
public class ConsumerLog {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLog.class);

    // 8 bytes timestamp + 8 bytes wrote offset + 4 bytes wrote bytes + 2 bytes header size
    private static final int CONSUMER_LOG_UNIT_BYTES = 22;

    // 2 bytes magic + 2 bytes payload type + 28 bytes payload
    // payload type 0xC100: 8 bytes timestamp + 8 bytes wrote offset + 4 bytes wrote bytes + 2 bytes header size + 6 bytes unused
    // payload type 0xC101: 8 bytes timestamp + 8 bytes tablet id + 4 bytes position + 4 bytes wrote bytes + 4 bytes unused
    private static final int CONSUMER_LOG_V2_UNIT_BYTES = 32;

    private static final int SEGMENT_TOTAL_UNIT = 10_000_000;

    private final StorageConfig config;
    private final String subject;
    private final boolean consumerLogV2Enable;
    private final int unitBytes;
    private final int segmentBytes;
    private final LogManager logManager;

    private final MessageAppender<MessageLogIndex, Void> messageLogIndexAppender;
    private final MessageAppender<MessageLogIndex, Void> messageLogIndexV2Appender;
    private final MessageAppender<SMTIndex, Void> smtIndexAppender;
    private final ReentrantLock writeGuard = new ReentrantLock();

    private volatile long minSequence;

    public ConsumerLog(final StorageConfig config, final String subject) {
        this(config, subject, -1);
    }

    public ConsumerLog(final StorageConfig config, final String subject, final long maxSequence) {
        this.config = config;
        this.subject = subject;
        this.consumerLogV2Enable = config.isConsumerLogV2Enable();
        if (consumerLogV2Enable) {
            this.unitBytes = CONSUMER_LOG_V2_UNIT_BYTES;
        } else {
            this.unitBytes = CONSUMER_LOG_UNIT_BYTES;
        }
        this.segmentBytes = unitBytes * SEGMENT_TOTAL_UNIT;
        this.logManager = new LogManager(new File(config.getConsumerLogStorePath(), subject),
                segmentBytes,
                new MaxSequenceLogSegmentValidator(maxSequence, unitBytes));

        this.messageLogIndexAppender = new MessageLogIndexAppender();
        this.messageLogIndexV2Appender = new MessageLogIndexV2Appender();
        this.smtIndexAppender = new SMTIndexAppender();
    }

    public int getUnitBytes() {
        return unitBytes;
    }

    public boolean writeMessageLogIndex(final long sequence, final long wroteOffset, final int wroteBytes, final short headerSize) {
        final MessageLogIndex index = new MessageLogIndex(System.currentTimeMillis(), wroteOffset, wroteBytes, headerSize);
        if (consumerLogV2Enable) {
            return writeUnit(sequence, index, messageLogIndexV2Appender);
        } else {
            return writeUnit(sequence, index, messageLogIndexAppender);
        }
    }

    public boolean writeSMTIndex(final long sequence, final long timestamp, final long tabletId, final int position, final int size) {
        return writeUnit(sequence, new SMTIndex(timestamp, tabletId, position, size), smtIndexAppender);
    }

    private <T> boolean writeUnit(final long sequence, final T unit, final MessageAppender<T, Void> appender) {
        writeGuard.lock();
        try {
            if (sequence < nextSequence()) {
                return true;
            }

            final long expectedOffset = sequence * unitBytes;
            LogSegment segment = logManager.locateSegment(expectedOffset);
            if (segment == null) {
                segment = logManager.allocOrResetSegments(expectedOffset);
            }
            fillPreBlank(segment, expectedOffset);

            final AppendMessageResult result = segment.append(unit, appender);
            switch (result.getStatus()) {
                case SUCCESS:
                    return true;
                case END_OF_FILE:
                    logManager.allocNextSegment();
                    return writeUnit(sequence, unit, appender);
                default:
                    return false;
            }
        } finally {
            writeGuard.unlock();
        }
    }

    private void fillPreBlank(final LogSegment segment, final long untilWhere) {
        final MessageLogIndex blankMessage = new MessageLogIndex(System.currentTimeMillis(), 0, Integer.MAX_VALUE, (short) 0);
        final long startOffset = segment.getBaseOffset() + segment.getWrotePosition();
        for (long i = startOffset; i < untilWhere; i += unitBytes) {
            if (consumerLogV2Enable) {
                segment.append(blankMessage, messageLogIndexV2Appender);
            } else {
                segment.append(blankMessage, messageLogIndexAppender);
            }
        }
    }

    public SegmentBuffer selectIndexBuffer(long startIndex) {
        final long startOffset = startIndex * unitBytes;
        final LogSegment segment = logManager.locateSegment(startOffset);
        if (segment == null) {
            QMon.hitDeletedConsumerLogSegmentCountInc(subject);
            return null;
        } else {
            return segment.selectSegmentBuffer((int) (startOffset % segmentBytes));
        }
    }

    public Unit readUnit(final ByteBuffer buffer) {
        if (!consumerLogV2Enable) {
            final MessageLogIndex index = new MessageLogIndex(buffer.getLong(), buffer.getLong(), buffer.getInt(), buffer.getShort());
            return Unit.fromMessageLogIndex(index);
        }

        final short magic = buffer.getShort();
        if (magic != MagicCode.CONSUMER_LOG_MAGIC_V2) {
            throw new RuntimeException("illegal consumer log magic code " + magic);
        }
        final short type = buffer.getShort();
        if (type == PayloadType.MESSAGE_LOG_INDEX.getCode()) {
            final long timestamp = buffer.getLong();
            final long wroteOffset = buffer.getLong();
            final int wroteSize = buffer.getInt();
            final short headerSize = buffer.getShort();
            // skip unused bytes
            buffer.getShort();
            buffer.getInt();
            return Unit.fromMessageLogIndex(new MessageLogIndex(timestamp, wroteOffset, wroteSize, headerSize));
        } else if (type == PayloadType.SMT_INDEX.getCode()) {
            final long timestamp = buffer.getLong();
            final long tabletId = buffer.getLong();
            final int position = buffer.getInt();
            final int size = buffer.getInt();
            // skip unused bytes
            buffer.getInt();
            return Unit.fromSMTIndex(new SMTIndex(timestamp, tabletId, position, size));
        } else {
            throw new RuntimeException("illegal consumer log unit payload type " + type);
        }
    }

    public void setMinSequence(long sequence) {
        long computedMinSequence = getMinOffset();
        if (computedMinSequence < sequence) {
            this.minSequence = sequence;
            QMon.adjustConsumerLogMinOffset(subject);
            LOG.info("adjust consumer log {} min offset from {} to {}.", subject, computedMinSequence, minSequence);
        }
    }

    public long getMinOffset() {
        long computedMinSequence = logManager.getMinOffset() / unitBytes;
        if (computedMinSequence < minSequence) {
            return minSequence;
        } else {
            return computedMinSequence;
        }
    }

    public long nextSequence() {
        return logManager.getMaxOffset() / unitBytes;
    }

    public OffsetBound getOffsetBound() {
        final long minOffset = getMinOffset();
        final long maxOffset = nextSequence();
        return new OffsetBound(Math.min(minOffset, maxOffset), maxOffset);
    }

    public void flush() {
        logManager.flush();
        QMon.flushConsumerLogCountInc(subject);
    }

    public void close() {
        logManager.close();
    }

    public void clean() {
        logManager.deleteExpiredSegments(config.getConsumerLogRetentionMs());
    }

    public enum PayloadType {
        MESSAGE_LOG_INDEX((short) 0xC100),
        SMT_INDEX((short) 0xC101);

        private final short code;

        PayloadType(final short code) {
            this.code = code;
        }

        public short getCode() {
            return code;
        }
    }

    public static class MessageLogIndex {
        private final long timestamp;
        private final long wroteOffset;
        private final int wroteBytes;
        private final short headerSize;

        private MessageLogIndex(long timestamp, long wroteOffset, int wroteBytes, short headerSize) {
            this.timestamp = timestamp;
            this.wroteOffset = wroteOffset;
            this.wroteBytes = wroteBytes;
            this.headerSize = headerSize;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getWroteOffset() {
            return wroteOffset;
        }

        public int getWroteBytes() {
            return wroteBytes;
        }

        public short getHeaderSize() {
            return headerSize;
        }
    }

    private static class MessageLogIndexAppender implements MessageAppender<MessageLogIndex, Void> {
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(CONSUMER_LOG_UNIT_BYTES);

        @Override
        public AppendMessageResult<Void> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, MessageLogIndex message) {
            workingBuffer.clear();

            final long wroteOffset = baseOffset + targetBuffer.position();
            workingBuffer.flip();
            workingBuffer.limit(CONSUMER_LOG_UNIT_BYTES);
            workingBuffer.putLong(message.getTimestamp());
            workingBuffer.putLong(message.getWroteOffset());
            workingBuffer.putInt(message.getWroteBytes());
            workingBuffer.putShort(message.getHeaderSize());
            targetBuffer.put(workingBuffer.array(), 0, CONSUMER_LOG_UNIT_BYTES);
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, CONSUMER_LOG_UNIT_BYTES);
        }
    }

    private static class MessageLogIndexV2Appender implements MessageAppender<MessageLogIndex, Void> {
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(CONSUMER_LOG_V2_UNIT_BYTES);

        @Override
        public AppendMessageResult<Void> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, MessageLogIndex message) {
            workingBuffer.clear();

            final long wroteOffset = baseOffset + targetBuffer.position();
            workingBuffer.limit(CONSUMER_LOG_V2_UNIT_BYTES);
            workingBuffer.putShort(MagicCode.CONSUMER_LOG_MAGIC_V2);
            workingBuffer.putShort(PayloadType.MESSAGE_LOG_INDEX.getCode());
            workingBuffer.putLong(System.currentTimeMillis());
            workingBuffer.putLong(message.getWroteOffset());
            workingBuffer.putInt(message.getWroteBytes());
            workingBuffer.putShort(message.getHeaderSize());
            workingBuffer.putShort((short) 0);
            workingBuffer.putInt(0);
            workingBuffer.flip();
            targetBuffer.put(workingBuffer);
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, CONSUMER_LOG_V2_UNIT_BYTES);
        }
    }

    public static final class SMTIndex {
        private final long timestamp;
        private final long tabletId;
        private final int position;
        private final int size;

        private SMTIndex(final long timestamp, final long tabletId, final int position, final int size) {
            this.timestamp = timestamp;
            this.tabletId = tabletId;
            this.position = position;
            this.size = size;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getTabletId() {
            return tabletId;
        }

        public int getPosition() {
            return position;
        }

        public int getSize() {
            return size;
        }
    }

    private static final class SMTIndexAppender implements MessageAppender<SMTIndex, Void> {
        private final ByteBuffer workingBuffer = ByteBuffer.allocateDirect(CONSUMER_LOG_V2_UNIT_BYTES);

        @Override
        public AppendMessageResult<Void> doAppend(final long baseOffset, final ByteBuffer targetBuffer, final int freeSpace, final SMTIndex index) {
            workingBuffer.clear();

            final long wroteOffset = baseOffset + targetBuffer.position();
            workingBuffer.limit(CONSUMER_LOG_V2_UNIT_BYTES);
            workingBuffer.putShort(MagicCode.CONSUMER_LOG_MAGIC_V2);
            workingBuffer.putShort(PayloadType.SMT_INDEX.getCode());
            workingBuffer.putLong(index.getTimestamp());
            workingBuffer.putLong(index.getTabletId());
            workingBuffer.putInt(index.getPosition());
            workingBuffer.putInt(index.getSize());
            workingBuffer.putInt(0);
            workingBuffer.flip();
            targetBuffer.put(workingBuffer);
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, CONSUMER_LOG_V2_UNIT_BYTES);
        }
    }

    public static final class Unit {
        private final PayloadType type;
        private final Object index;

        private Unit(final PayloadType type, final Object index) {
            this.type = type;
            this.index = index;
        }

        public static Unit fromMessageLogIndex(final MessageLogIndex index) {
            return new Unit(PayloadType.MESSAGE_LOG_INDEX, index);
        }

        public static Unit fromSMTIndex(final SMTIndex index) {
            return new Unit(PayloadType.SMT_INDEX, index);
        }

        public PayloadType getType() {
            return type;
        }

        public Object getIndex() {
            return index;
        }
    }
}
