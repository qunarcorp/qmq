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
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.utils.Crc32;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author keli.wang
 * @since 2017/7/4
 */
public class MessageLog implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageLog.class);

    private static final int PER_SEGMENT_FILE_SIZE = 1024 * 1024 * 1024;

    //4 bytes magic code + 1 byte attribute + 8 bytes timestamp
    public static final int MIN_RECORD_BYTES = 13;

    private final StorageConfig config;
    private final ConsumerLogManager consumerLogManager;
    private final LogManager logManager;
    private final MessageAppender<RawMessage, MessageSequence> messageAppender = new RawMessageAppender();

    public MessageLog(final StorageConfig config, final ConsumerLogManager consumerLogManager) {
        this.config = config;
        this.consumerLogManager = consumerLogManager;
        this.logManager = new LogManager(new File(config.getMessageLogStorePath()), PER_SEGMENT_FILE_SIZE, config, new MessageLogSegmentValidator());
        consumerLogManager.adjustConsumerLogMinOffset(logManager.firstSegment());
    }

    private static int recordSize(final int subjectSize, final int payloadSize) {
        return 4 // magic code
                + 1 // attributes
                + 8 // timestamp
                + 8 // message logical offset
                + 2 // subject size
                + (subjectSize > 0 ? subjectSize : 0)
                + 8 // payload crc32
                + 4 // payload size
                + (payloadSize > 0 ? payloadSize : 0);
    }

    public long getMaxOffset() {
        return logManager.getMaxOffset();
    }

    public long getMinOffset() {
        return logManager.getMinOffset();
    }

    public PutMessageResult putMessage(final RawMessage message) {
        final AppendMessageResult<MessageSequence> result;
        LogSegment segment = logManager.latestSegment();
        if (segment == null) {
            segment = logManager.allocNextSegment();
        }

        if (segment == null) {
            return new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
        }

        result = segment.append(message, messageAppender);
        switch (result.getStatus()) {
            case SUCCESS:
                break;
            case END_OF_FILE:
                LogSegment logSegment = logManager.allocNextSegment();
                if (logSegment == null) {
                    return new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
                }
                return putMessage(message);
            case MESSAGE_SIZE_EXCEEDED:
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
            default:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
        }

        return new PutMessageResult(PutMessageStatus.SUCCESS, result);
    }

    public SegmentBuffer getMessage(final long wroteOffset, final int wroteBytes, final short headerSize) {
        long payloadOffset = wroteOffset + headerSize;
        final LogSegment segment = logManager.locateSegment(payloadOffset);
        if (segment == null) return null;

        final int payloadSize = wroteBytes - headerSize;
        final int pos = (int) (payloadOffset % PER_SEGMENT_FILE_SIZE);
        final SegmentBuffer result = segment.selectSegmentBuffer(pos, payloadSize);
        if (result == null) return null;

        result.setWroteOffset(wroteOffset);
        return result;
    }

    public SegmentBuffer getMessageData(final long offset) {
        final LogSegment segment = logManager.locateSegment(offset);
        if (segment == null) return null;

        final int pos = (int) (offset % PER_SEGMENT_FILE_SIZE);
        return segment.selectSegmentBuffer(pos);
    }

    public boolean appendData(final long startOffset, final ByteBuffer data) {
        LogSegment segment = logManager.locateSegment(startOffset);
        if (segment == null) {
            segment = logManager.allocOrResetSegments(startOffset);
            fillPreBlank(segment, startOffset);
        }

        return segment.appendData(data);
    }

    private void fillPreBlank(LogSegment segment, long untilWhere) {
        final ByteBuffer buffer = ByteBuffer.allocate(17);
        buffer.putInt(MagicCode.MESSAGE_LOG_MAGIC_V3);
        buffer.put((byte) 2);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt((int) (untilWhere % PER_SEGMENT_FILE_SIZE));
        segment.fillPreBlank(buffer, untilWhere);
    }

    public void flush() {
        final long start = System.currentTimeMillis();
        try {
            logManager.flush();
        } finally {
            QMon.flushMessageLogTimer(System.currentTimeMillis() - start);
        }
    }

    @Override
    public void close() {
        logManager.close();
    }

    public void clean() {
        logManager.deleteExpiredSegments(config.getMessageLogRetentionMs(), segment -> {
            consumerLogManager.adjustConsumerLogMinOffset(logManager.firstSegment());

            final String fileName = StoreUtils.offsetFileNameForSegment(segment);
            final String path = config.getMessageLogStorePath();
            final File file = new File(path, fileName);
            try {
                if (!file.delete()) {
                    LOG.warn("delete offset file failed. file: {}", fileName);
                }
            } catch (Exception e) {
                LOG.warn("delete offset file failed.. file: {}", fileName, e);
            }
        });
    }

    public MessageLogMetaVisitor newVisitor(long iterateFrom) {
        return new MessageLogMetaVisitor(logManager, iterateFrom);
    }

    private static class MessageLogSegmentValidator implements LogSegmentValidator {
        @Override
        public ValidateResult validate(LogSegment segment) {
            final int fileSize = segment.getFileSize();
            final ByteBuffer buffer = segment.sliceByteBuffer();

            int position = 0;
            while (true) {
                if (position == fileSize) {
                    return new ValidateResult(ValidateStatus.COMPLETE, fileSize);
                }

                final int result = consumeAndValidateMessage(segment, buffer);
                if (result == -1) {
                    return new ValidateResult(ValidateStatus.PARTIAL, position);
                } else if (result == 0) {
                    return new ValidateResult(ValidateStatus.COMPLETE, fileSize);
                } else {
                    position += result;
                }
            }
        }

        private int consumeAndValidateMessage(final LogSegment segment, final ByteBuffer buffer) {
            final int magic = buffer.getInt();
            if (!MagicCodeSupport.isValidMessageLogMagicCode(magic)) {
                return -1;
            }

            final byte attributes = buffer.get();
            buffer.getLong();
            if (attributes == 2) {
                return buffer.getInt();
            } else if (attributes == 1) {
                return 0;
            } else if (attributes == 0) {
                buffer.getLong();
                final short subjectSize = buffer.getShort();
                buffer.position(buffer.position() + subjectSize);
                final long crc = buffer.getLong();
                final int payloadSize = buffer.getInt();
                final byte[] payload = new byte[payloadSize];
                buffer.get(payload);
                final long computedCrc = Crc32.crc32(payload);
                if (computedCrc == crc) {
                    return recordSize(subjectSize, payloadSize);
                } else {
                    LOG.warn("crc check failed. stored crc: {}, computed crc: {}, segment: {}", crc, computedCrc, segment);
                    return -1;
                }
            } else {
                return -1;
            }
        }
    }

    private class RawMessageAppender implements MessageAppender<RawMessage, MessageSequence> {
        private static final int MAX_BYTES = 1024;

        private static final byte ATTR_EMPTY_RECORD = 1;
        private static final byte ATTR_MESSAGE_RECORD = 0;

        private final ByteBuffer workingBuffer = ByteBuffer.allocate(MAX_BYTES);

        @Override
        public AppendMessageResult<MessageSequence> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, RawMessage message) {
            workingBuffer.clear();

            final String subject = message.getHeader().getSubject();
            final byte[] subjectBytes = subject.getBytes(StandardCharsets.UTF_8);

            final long wroteOffset = baseOffset + targetBuffer.position();
            final int recordSize = recordSize(subjectBytes.length, message.getBodySize());

            if (recordSize != freeSpace && recordSize + MIN_RECORD_BYTES > freeSpace) {
                workingBuffer.limit(MIN_RECORD_BYTES);
                workingBuffer.putInt(MagicCode.MESSAGE_LOG_MAGIC_V3);
                workingBuffer.put(ATTR_EMPTY_RECORD);
                workingBuffer.putLong(System.currentTimeMillis());
                targetBuffer.put(workingBuffer.array(), 0, MIN_RECORD_BYTES);
                int fillZeroLen = freeSpace - MIN_RECORD_BYTES;
                if (fillZeroLen > 0) {
                    targetBuffer.put(fillZero(fillZeroLen));
                }
                return new AppendMessageResult<>(AppendMessageStatus.END_OF_FILE, wroteOffset, freeSpace, null);
            } else {
                final long sequence = consumerLogManager.getOffsetOrDefault(subject, 0);

                int headerSize = recordSize - message.getBodySize();
                workingBuffer.limit(headerSize);
                workingBuffer.putInt(MagicCode.MESSAGE_LOG_MAGIC_V3);
                workingBuffer.put(ATTR_MESSAGE_RECORD);
                workingBuffer.putLong(System.currentTimeMillis());
                workingBuffer.putLong(sequence);
                workingBuffer.putShort((short) subjectBytes.length);
                workingBuffer.put(subjectBytes);
                workingBuffer.putLong(message.getHeader().getBodyCrc());
                workingBuffer.putInt(message.getBodySize());
                targetBuffer.put(workingBuffer.array(), 0, headerSize);
                targetBuffer.put(message.getBody().nioBuffer());

                consumerLogManager.incOffset(subject);

                final long payloadOffset = wroteOffset + headerSize;
                return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, recordSize, new MessageSequence(sequence, payloadOffset));
            }
        }

        private byte[] fillZero(int len) {
            byte[] zero = new byte[len];
            Arrays.fill(zero, (byte) 0);
            return zero;
        }
    }
}
