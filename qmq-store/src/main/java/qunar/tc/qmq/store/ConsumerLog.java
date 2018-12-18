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
    static final int CONSUMER_LOG_UNIT_BYTES = 22;
    private static final int CONSUMER_LOG_SIZE = CONSUMER_LOG_UNIT_BYTES * 10000000;

    private final StorageConfig config;
    private final String subject;
    private final LogManager logManager;
    private final MessageAppender<ConsumerLogMessage, Void> consumerLogAppender = new ConsumerLogMessageAppender();
    private final ReentrantLock putMessageLogOffsetLock = new ReentrantLock();

    private volatile long minSequence;

    public ConsumerLog(final StorageConfig config, final String subject) {
        this.config = config;
        this.subject = subject;
        this.logManager = new LogManager(new File(config.getConsumerLogStorePath(), subject), CONSUMER_LOG_SIZE, config, new ConsumerLogSegmentValidator());
    }

    // TODO(keli.wang): handle write fail and retry
    public boolean putMessageLogOffset(final long sequence, final long offset, final int size, final short headerSize) {
        putMessageLogOffsetLock.lock();
        try {
            if (sequence < nextSequence()) {
                return true;
            }

            final long expectedOffset = sequence * CONSUMER_LOG_UNIT_BYTES;
            LogSegment segment = logManager.locateSegment(expectedOffset);
            if (segment == null) {
                segment = logManager.allocOrResetSegments(expectedOffset);
            }
            fillPreBlank(segment, expectedOffset);

            final AppendMessageResult result = segment.append(new ConsumerLogMessage(sequence, offset, size, headerSize), consumerLogAppender);
            switch (result.getStatus()) {
                case SUCCESS:
                    break;
                case END_OF_FILE:
                    logManager.allocNextSegment();
                    return putMessageLogOffset(sequence, offset, size, headerSize);
                default:
                    return false;
            }
        } finally {
            putMessageLogOffsetLock.unlock();
        }

        return true;
    }

    private void fillPreBlank(final LogSegment segment, final long untilWhere) {
        final ConsumerLogMessage blankMessage = new ConsumerLogMessage(0, 0, Integer.MAX_VALUE, (short) 0);
        final long startOffset = segment.getBaseOffset() + segment.getWrotePosition();
        for (long i = startOffset; i < untilWhere; i += CONSUMER_LOG_UNIT_BYTES) {
            segment.append(blankMessage, consumerLogAppender);
        }
    }

    public SegmentBuffer selectIndexBuffer(long startIndex) {
        final long startOffset = startIndex * CONSUMER_LOG_UNIT_BYTES;
        final LogSegment segment = logManager.locateSegment(startOffset);
        if (segment == null) {
            QMon.hitDeletedConsumerLogSegmentCountInc(subject);
            return null;
        } else {
            return segment.selectSegmentBuffer((int) (startOffset % CONSUMER_LOG_SIZE));
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
        long computedMinSequence = logManager.getMinOffset() / CONSUMER_LOG_UNIT_BYTES;
        if (computedMinSequence < minSequence) {
            return minSequence;
        } else {
            return computedMinSequence;
        }
    }

    public long nextSequence() {
        return logManager.getMaxOffset() / CONSUMER_LOG_UNIT_BYTES;
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

    private static class ConsumerLogMessage {
        private final long sequence;
        private final long offset;
        private final int size;
        private final short headerSize;

        private ConsumerLogMessage(long sequence, long offset, int size, short headerSize) {
            this.sequence = sequence;
            this.offset = offset;
            this.size = size;
            this.headerSize = headerSize;
        }

        public long getSequence() {
            return sequence;
        }

        public long getOffset() {
            return offset;
        }

        public int getSize() {
            return size;
        }

        public short getHeaderSize() {
            return headerSize;
        }
    }

    private static class ConsumerLogMessageAppender implements MessageAppender<ConsumerLogMessage, Void> {
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(CONSUMER_LOG_UNIT_BYTES);

        @Override
        public AppendMessageResult<Void> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, ConsumerLogMessage message) {
            workingBuffer.clear();

            final long wroteOffset = baseOffset + targetBuffer.position();
            workingBuffer.flip();
            workingBuffer.limit(CONSUMER_LOG_UNIT_BYTES);
            workingBuffer.putLong(System.currentTimeMillis());
            workingBuffer.putLong(message.getOffset());
            workingBuffer.putInt(message.getSize());
            workingBuffer.putShort(message.getHeaderSize());
            targetBuffer.put(workingBuffer.array(), 0, CONSUMER_LOG_UNIT_BYTES);
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, CONSUMER_LOG_UNIT_BYTES);
        }
    }

    private static class ConsumerLogSegmentValidator implements LogSegmentValidator {
        @Override
        public ValidateResult validate(LogSegment segment) {
            final int fileSize = segment.getFileSize();
            final ByteBuffer buffer = segment.sliceByteBuffer();

            int position = 0;
            while (true) {
                if (position == fileSize) {
                    return new ValidateResult(ValidateStatus.COMPLETE, fileSize);
                }

                final int result = consumeAndValidateMessage(buffer);
                if (result == -1) {
                    return new ValidateResult(ValidateStatus.PARTIAL, position);
                } else {
                    position += result;
                }
            }
        }

        private int consumeAndValidateMessage(final ByteBuffer buffer) {
            final long timestamp = buffer.getLong();
            final long offset = buffer.getLong();
            final int size = buffer.getInt();
            final short headerSize = buffer.getShort();
            if (isBlankMessage(timestamp, offset, size, headerSize)) {
                return CONSUMER_LOG_UNIT_BYTES;
            } else if (isValidMessage(timestamp, offset, size, headerSize)) {
                return CONSUMER_LOG_UNIT_BYTES;
            } else {
                return -1;
            }
        }

        private boolean isBlankMessage(final long timestamp, final long offset, final int size, final short headerSize) {
            return timestamp > 0 && offset == 0 && headerSize == 0 && size == Integer.MAX_VALUE;
        }

        private boolean isValidMessage(final long timestamp, final long offset, final int size, final short headerSize) {
            return timestamp > 0 && offset >= 0 && size > 0 && headerSize <= size && headerSize >= MessageLog.MIN_RECORD_BYTES;
        }
    }
}
