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

package qunar.tc.qmq.delay.store.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.config.StoreConfiguration;
import qunar.tc.qmq.delay.store.model.AppendMessageRecordResult;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.MessageLogAttrEnum;
import qunar.tc.qmq.delay.store.model.RawMessageExtend;
import qunar.tc.qmq.delay.store.visitor.DelayMessageLogVisitor;
import qunar.tc.qmq.delay.store.visitor.LogVisitor;
import qunar.tc.qmq.store.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.delay.store.model.MessageLogAttrEnum.*;
import static qunar.tc.qmq.store.MagicCode.MESSAGE_LOG_MAGIC_V1;
import static qunar.tc.qmq.store.MagicCode.MESSAGE_LOG_MAGIC_V2;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 10:06
 */
public class MessageSegmentContainer implements SegmentContainer<AppendMessageRecordResult, RawMessageExtend> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSegmentContainer.class);

    private static final int MIN_RECORD_BYTES = 13;

    private final LogManager logManager;
    private final MessageAppender<RawMessageExtend, Long> messageAppender;
    private final StoreConfiguration config;
    private final AtomicLong sequence = new AtomicLong(0);

    MessageSegmentContainer(StoreConfiguration config) {
        this.config = config;
        this.messageAppender = new MessageSegmentContainer.DelayRawMessageAppender();
        this.logManager = new LogManager(new File(config.getMessageLogStorePath())
                , config.getMessageLogSegmentFileSize()
                , new StorageConfigImpl(config.getConfig())
                , new MessageLogSegmentValidator());
        recoverSequence();
    }

    private void recoverSequence() {
        LogSegment logSegment = logManager.latestSegment();
        if (null == logSegment) {
            LOGGER.warn("recover sequence happened null segment");
            return;
        }
        validateSequence(logSegment);
    }

    private void validateSequence(LogSegment logSegment) {
        LOGGER.info("validate logs.");
        final int fileSize = logSegment.getFileSize();
        final ByteBuffer buffer = logSegment.sliceByteBuffer();

        int position = 0;
        while (true) {
            if (position == fileSize) {
                break;
            }

            final int result = doValidateSequence(buffer, sequence.get());
            if (result == -1) {
                break;
            } else {
                position += result;
            }
        }

        LOGGER.info("validate logs done.");
    }

    private int doValidateSequence(final ByteBuffer buffer, long lastSequence) {
        try {
            final int magic = buffer.getInt();
            if (!MagicCodeSupport.isValidMessageLogMagicCode(magic)) {
                return -1;
            }

            final byte attributes = buffer.get();
            buffer.getLong();
            if (attributes == ATTR_SKIP_RECORD.getCode()) {
                return buffer.getInt();
            } else if (attributes == ATTR_MESSAGE_RECORD.getCode()) {
                return resolveSequence(buffer, magic, lastSequence);
            } else {
                return -1;
            }
        } catch (Exception e) {
            LOGGER.error("message log recover resolve sequence error", e);
            return -1;
        }
    }

    private int resolveSequence(final ByteBuffer buffer, final int magic, final long lastSequence) {
        try {
            buffer.getLong();
            long nextSequence = buffer.getLong();
            final int messageIdSize = buffer.getInt();
            buffer.position(buffer.position() + messageIdSize);
            final int subjectSize = buffer.getInt();
            buffer.position(buffer.position() + subjectSize);
            if (magic >= MESSAGE_LOG_MAGIC_V2) {
                buffer.getLong();
                final int payloadSize = buffer.getInt();
                buffer.position(buffer.position() + payloadSize);
                sequence.set(Math.max(nextSequence, lastSequence));
                return recordSizeWithCrc(messageIdSize, subjectSize, payloadSize);
            } else {
                final int payloadSize = buffer.getInt();
                buffer.position(buffer.position() + payloadSize);
                sequence.set(Math.max(nextSequence, lastSequence));
                return recordSize(messageIdSize, subjectSize, payloadSize);
            }
        } catch (Exception e) {
            LOGGER.error("message log recover complete record, resolve sequence error", e);
            return -1;
        }
    }

    @Override
    public AppendMessageRecordResult append(RawMessageExtend record) {
        AppendMessageResult<Long> result;
        LogSegment segment = logManager.latestSegment();
        if (null == segment) {
            segment = logManager.allocNextSegment();
        }

        if (null == segment) {
            return new AppendMessageRecordResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
        }

        result = segment.append(record, messageAppender);
        switch (result.getStatus()) {
            case MESSAGE_SIZE_EXCEEDED:
                return new AppendMessageRecordResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            case END_OF_FILE:
                if (null == logManager.allocNextSegment()) {
                    return new AppendMessageRecordResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
                }
                return append(record);
            case SUCCESS:
                return new AppendMessageRecordResult(PutMessageStatus.SUCCESS, result);
            default:
                return new AppendMessageRecordResult(PutMessageStatus.UNKNOWN_ERROR, result);
        }
    }

    @Override
    public boolean clean(Long key) {
        return logManager.clean(key);
    }

    public void clean() {
        logManager.deleteExpiredSegments(config.getMessageLogRetentionMs());
    }

    @Override
    public void flush() {
        logManager.flush();
    }

    LogVisitor<LogRecord> newLogVisitor(final Long key) {
        return new DelayMessageLogVisitor(logManager, key);
    }

    long getMaxOffset() {
        return logManager.getMaxOffset();
    }

    /**
     * 4 // magic code
     * 1 // attributes
     * 8 // timestamp
     * 8 // schedule time
     * 8 // sequence
     * 4 // messageId size
     * ((messageIdSize > 0) ? messageIdSize : 0)
     * 4 // subject size
     * ((subjectSize > 0) ? subjectSize : 0)
     * 8 // payload crc32
     * 4 // payload size
     * ((payloadSize > 0) ? payloadSize : 0);
     *
     * @param subjectSize 主题大小
     * @param payloadSize 内容大小
     * @return 记录大小
     */
    private static int recordSizeWithCrc(final int messageIdSize, final int subjectSize, final int payloadSize) {
        return 4 + 1
                + 8
                + 8
                + 8
                + 4
                + ((messageIdSize > 0) ? messageIdSize : 0)
                + 4
                + ((subjectSize > 0) ? subjectSize : 0)
                + 8
                + 4
                + ((payloadSize > 0) ? payloadSize : 0);
    }

    /**
     * 4 // magic code
     * 1 // attributes
     * 8 // timestamp
     * 8 // schedule time
     * 8 // sequence
     * 4 // messageId size
     * ((messageIdSize > 0) ? messageIdSize : 0)
     * 4 // subject size
     * ((subjectSize > 0) ? subjectSize : 0)
     * 4 // payload size
     * ((payloadSize > 0) ? payloadSize : 0);
     *
     * @param subjectSize 主题大小
     * @param payloadSize 内容大小
     * @return 记录大小
     */
    private static int recordSize(final int messageIdSize, final int subjectSize, final int payloadSize) {
        return 4 + 1
                + 8
                + 8
                + 8
                + 4
                + ((messageIdSize > 0) ? messageIdSize : 0)
                + 4
                + (subjectSize > 0 ? subjectSize : 0)
                + 4
                + (payloadSize > 0 ? payloadSize : 0);
    }

    long getMinOffset() {
        return logManager.getMinOffset();
    }

    SegmentBuffer getMessageData(long startSyncOffset) {
        LogSegment segment = logManager.locateSegment(startSyncOffset);
        if (null == segment) {
            return null;
        }

        return segment.selectSegmentBuffer((int) (startSyncOffset % config.getMessageLogSegmentFileSize()));
    }

    boolean appendData(long startOffset, ByteBuffer buffer) {
        LogSegment segment = logManager.locateSegment(startOffset);
        if (null == segment) {
            segment = logManager.allocOrResetSegments(startOffset);
            fillPreBlank(segment, startOffset);
        }
        return segment.appendData(buffer);
    }

    private void fillPreBlank(LogSegment segment, long untilWhere) {
        final ByteBuffer buffer = ByteBuffer.allocate(17);
        buffer.putInt(MagicCode.MESSAGE_LOG_MAGIC_V2);
        buffer.put((byte) 2);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt((int) (untilWhere % config.getMessageLogSegmentFileSize()));
        segment.fillPreBlank(buffer, untilWhere);
    }

    private class DelayRawMessageAppender implements MessageAppender<RawMessageExtend, Long> {
        private final ReentrantLock lock = new ReentrantLock();
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(1024);

        @Override
        public AppendMessageResult<Long> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, RawMessageExtend message) {
            lock.lock();
            try {
                workingBuffer.clear();

                final String messageId = message.getHeader().getMessageId();
                final byte[] messageIdBytes = messageId.getBytes(StandardCharsets.UTF_8);
                final String subject = message.getHeader().getSubject();
                final byte[] subjectBytes = subject.getBytes(StandardCharsets.UTF_8);

                final long startWroteOffset = baseOffset + targetBuffer.position();
                final int recordSize = recordSizeWithCrc(messageIdBytes.length, subjectBytes.length, message.getBodySize());

                if (recordSize > config.getSingleMessageLimitSize()) {
                    return new AppendMessageResult<>(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, startWroteOffset, freeSpace, null);
                }
                if (recordSize != freeSpace && recordSize + MIN_RECORD_BYTES > freeSpace) {
                    workingBuffer.limit(MIN_RECORD_BYTES);
                    workingBuffer.putInt(MESSAGE_LOG_MAGIC_V1);
                    workingBuffer.put(MessageLogAttrEnum.ATTR_EMPTY_RECORD.getCode());
                    workingBuffer.putLong(System.currentTimeMillis());
                    targetBuffer.put(workingBuffer.array(), 0, MIN_RECORD_BYTES);
                    int fillZeroLen = freeSpace - MIN_RECORD_BYTES;
                    if (fillZeroLen > 0) {
                        targetBuffer.put(fillZero(fillZeroLen));
                    }
                    return new AppendMessageResult<>(AppendMessageStatus.END_OF_FILE, startWroteOffset, freeSpace, null);
                } else {
                    int headerSize = recordSize - message.getBodySize();
                    workingBuffer.limit(headerSize);
                    workingBuffer.putInt(MESSAGE_LOG_MAGIC_V2);
                    workingBuffer.put(MessageLogAttrEnum.ATTR_MESSAGE_RECORD.getCode());
                    workingBuffer.putLong(System.currentTimeMillis());
                    workingBuffer.putLong(message.getScheduleTime());
                    workingBuffer.putLong(sequence.incrementAndGet());
                    workingBuffer.putInt(messageIdBytes.length);
                    workingBuffer.put(messageIdBytes);
                    workingBuffer.putInt(subjectBytes.length);
                    workingBuffer.put(subjectBytes);
                    workingBuffer.putLong(message.getHeader().getBodyCrc());
                    workingBuffer.putInt(message.getBodySize());
                    targetBuffer.put(workingBuffer.array(), 0, headerSize);
                    targetBuffer.put(message.getBody().nioBuffer());

                    final long payloadOffset = startWroteOffset + headerSize;
                    return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, startWroteOffset, recordSize, payloadOffset);
                }
            } finally {
                lock.unlock();
            }
        }

        private byte[] fillZero(int len) {
            byte[] zero = new byte[len];
            Arrays.fill(zero, (byte) 0);
            return zero;
        }
    }

    private static class MessageLogSegmentValidator implements LogSegmentValidator {

        MessageLogSegmentValidator() {
        }

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
                } else if (result == 0) {
                    return new ValidateResult(ValidateStatus.COMPLETE, fileSize);
                } else {
                    position += result;
                }
            }
        }

        private int consumeAndValidateMessage(final ByteBuffer buffer) {
            final int magic = buffer.getInt();
            if (magic != MESSAGE_LOG_MAGIC_V1 && magic != MESSAGE_LOG_MAGIC_V2) {
                return -1;
            }

            final byte attributes = buffer.get();
            buffer.getLong();
            if (attributes == ATTR_SKIP_RECORD.getCode()) {
                return buffer.getInt();
            } else if (attributes == ATTR_EMPTY_RECORD.getCode()) {
                return 0;
            } else if (attributes == ATTR_MESSAGE_RECORD.getCode()) {
                return resolveRecordSize(buffer, magic);
            } else {
                return -1;
            }
        }

        private int resolveRecordSize(final ByteBuffer buffer, final int magic) {
            buffer.getLong();
            buffer.getLong();
            final int messageIdSize = buffer.getInt();
            buffer.position(buffer.position() + messageIdSize);
            final int subjectSize = buffer.getInt();
            buffer.position(buffer.position() + subjectSize);
            if (magic >= MESSAGE_LOG_MAGIC_V2) {
                buffer.getLong();
                final int payloadSize = buffer.getInt();
                final byte[] payload = new byte[payloadSize];
                buffer.get(payload);
                return recordSizeWithCrc(messageIdSize, subjectSize, payloadSize);
            } else {
                final int payloadSize = buffer.getInt();
                buffer.position(buffer.position() + payloadSize);
                return recordSize(messageIdSize, subjectSize, payloadSize);
            }
        }
    }
}
