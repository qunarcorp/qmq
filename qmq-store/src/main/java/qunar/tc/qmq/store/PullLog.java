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

import qunar.tc.qmq.monitor.QMon;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/8/2
 */
public class PullLog {
    private static final int PULL_LOG_UNIT_BYTES = 8; // 8 bytes message sequence
    private static final int PULL_LOG_SIZE = PULL_LOG_UNIT_BYTES * 10_000_000; // TODO(keli.wang): to config

    private final StorageConfig config;
    private final LogManager logManager;
    private final MessageAppender<PullLogMessage, MessageSequence> messageAppender = new PullLogMessageAppender();

    public PullLog(final StorageConfig config, final String consumerId, final String groupAndSubject) {
        this.config = config;
        this.logManager = new LogManager(buildPullLogPath(consumerId, groupAndSubject), PULL_LOG_SIZE, config, new PullLogSegmentValidator());
    }

    private File buildPullLogPath(final String consumerId, final String groupAndSubject) {
        return new File(new File(config.getPullLogStorePath(), consumerId), groupAndSubject);
    }

    public synchronized List<PutMessageResult> putPullLogMessages(final List<PullLogMessage> messages) {
        final List<PutMessageResult> results = new ArrayList<>(messages.size());
        for (final PullLogMessage message : messages) {
            results.add(directPutMessage(message));
        }
        return results;
    }

    private PutMessageResult directPutMessage(final PullLogMessage message) {
        final long sequence = message.getSequence();

        if (sequence < getMaxOffset()) {
            return new PutMessageResult(PutMessageStatus.ALREADY_WRITTEN, null);
        }

        final long expectPhysicalOffset = sequence * PULL_LOG_UNIT_BYTES;
        LogSegment segment = logManager.locateSegment(expectPhysicalOffset);
        if (segment == null) {
            segment = logManager.allocOrResetSegments(expectPhysicalOffset);
        }
        fillPreBlank(segment, expectPhysicalOffset);

        final AppendMessageResult<MessageSequence> result = segment.append(message, messageAppender);
        switch (result.getStatus()) {
            case SUCCESS:
                break;
            case END_OF_FILE:
                logManager.allocNextSegment();
                return directPutMessage(message);
            default:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
        }

        return new PutMessageResult(PutMessageStatus.SUCCESS, result);
    }

    private void fillPreBlank(final LogSegment segment, final long untilWhere) {
        final PullLogMessage blankMessage = new PullLogMessage(0, -1);
        final long startOffset = segment.getBaseOffset() + segment.getWrotePosition();
        for (long i = startOffset; i < untilWhere; i += PULL_LOG_UNIT_BYTES) {
            segment.append(blankMessage, messageAppender);
        }
    }

    public long getMessageSequence(long pullLogSequence) {
        final SegmentBuffer result = selectIndexBuffer(pullLogSequence);
        if (result == null) {
            return -1;
        }

        if (!result.retain()) {
            return -1;
        }

        try {
            final ByteBuffer buffer = result.getBuffer();
            buffer.getInt();
            return buffer.getLong();
        } finally {
            result.release();
        }
    }

    private SegmentBuffer selectIndexBuffer(final long startIndex) {
        final long startOffset = startIndex * PULL_LOG_UNIT_BYTES;
        final LogSegment segment = logManager.locateSegment(startOffset);
        if (segment == null) {
            return null;
        } else {
            return segment.selectSegmentBuffer((int) (startOffset % PULL_LOG_SIZE));
        }
    }

    public long getMinOffset() {
        return logManager.getMinOffset() / PULL_LOG_UNIT_BYTES;
    }

    public long getMaxOffset() {
        return logManager.getMaxOffset() / PULL_LOG_UNIT_BYTES;
    }

    public void flush() {
        logManager.flush();
        QMon.flushPullLogCountInc();
    }

    public void close() {
        logManager.close();
    }

    public void clean(long sequence) {
        long offset = sequence * PULL_LOG_UNIT_BYTES;
        logManager.deleteSegmentsBeforeOffset(offset);
    }

    public void destroy() {
        logManager.destroy();
    }


    private static class PullLogMessageAppender implements MessageAppender<PullLogMessage, MessageSequence> {
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(PULL_LOG_UNIT_BYTES);

        @Override
        public AppendMessageResult<MessageSequence> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, PullLogMessage message) {
            workingBuffer.clear();

            final long wroteOffset = baseOffset + targetBuffer.position();
            workingBuffer.flip();
            workingBuffer.limit(PULL_LOG_UNIT_BYTES);
            workingBuffer.putLong(message.getMessageSequence());
            targetBuffer.put(workingBuffer.array(), 0, PULL_LOG_UNIT_BYTES);

            final long messageIndex = wroteOffset / PULL_LOG_UNIT_BYTES;
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, PULL_LOG_UNIT_BYTES, new MessageSequence(messageIndex, wroteOffset));
        }
    }

    private static class PullLogSegmentValidator implements LogSegmentValidator {
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
            buffer.getLong();
            return PULL_LOG_UNIT_BYTES;
        }
    }
}
