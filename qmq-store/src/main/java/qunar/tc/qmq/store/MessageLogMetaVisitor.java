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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public class MessageLogMetaVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(MessageLogMetaVisitor.class);

    private final LogSegment currentSegment;
    private final SegmentBuffer currentBuffer;
    private final AtomicInteger visitedBufferSize = new AtomicInteger(0);
    private final long startOffset;

    public MessageLogMetaVisitor(final LogManager logManager, final long startOffset) {
        final long initialOffset = initialOffset(logManager, startOffset);
        this.currentSegment = logManager.locateSegment(initialOffset);
        this.currentBuffer = selectBuffer(initialOffset);
        this.startOffset = initialOffset;
    }

    private long initialOffset(final LogManager logManager, final long originStart) {
        if (originStart < logManager.getMinOffset()) {
            LOG.error("initial message log visitor offset less than min offset. start: {}, min: {}",
                    originStart, logManager.getMinOffset());
            return logManager.getMinOffset();
        }

        return originStart;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public Optional<MessageLogMeta> nextRecord() {
        if (currentBuffer == null) {
            return null;
        }

        return readOneRecord(currentBuffer.getBuffer());
    }

    public int visitedBufferSize() {
        if (currentBuffer == null) {
            return 0;
        }
        return visitedBufferSize.get();
    }

    private SegmentBuffer selectBuffer(final long startOffset) {
        if (currentSegment == null) {
            return null;
        }
        final int pos = (int) (startOffset % currentSegment.getFileSize());
        return currentSegment.selectSegmentBuffer(pos);
    }

    private Optional<MessageLogMeta> readOneRecord(final ByteBuffer buffer) {
        if (buffer.remaining() < MessageLog.MIN_RECORD_BYTES) {
            return null;
        }

        final int startPos = buffer.position();
        final long wroteOffset = startOffset + startPos;
        final int magic = buffer.getInt();
        if (!MagicCodeSupport.isValidMessageLogMagicCode(magic)) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        }

        final byte attributes = buffer.get();
        //skip timestamp
        buffer.getLong();
        if (attributes == 2) {
            if (buffer.remaining() < Integer.BYTES) {
                return null;
            }
            final int blankSize = buffer.getInt();
            visitedBufferSize.addAndGet(blankSize + (buffer.position() - startPos));
            return Optional.empty();
        } else if (attributes == 1) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        } else {
            if (buffer.remaining() < (Long.BYTES + Integer.BYTES)) {
                return null;
            }
            final long sequence = buffer.getLong();

            final short subjectSize = buffer.getShort();
            if (buffer.remaining() < subjectSize) {
                return null;
            }
            final byte[] subjectBytes = new byte[subjectSize];
            buffer.get(subjectBytes);

            if (buffer.remaining() < Long.BYTES) {
                return null;
            }
            //skip crc
            buffer.getLong();

            if (buffer.remaining() < Integer.BYTES) {
                return null;
            }
            final int payloadSize = buffer.getInt();
            if (buffer.remaining() < payloadSize) {
                return null;
            }

            final short headerSize = (short) (buffer.position() - startPos);
            buffer.position(buffer.position() + payloadSize);
            final int wroteBytes = buffer.position() - startPos;
            visitedBufferSize.addAndGet(wroteBytes);
            return Optional.of(new MessageLogMeta(new String(subjectBytes, StandardCharsets.UTF_8),
                    sequence, wroteOffset, wroteBytes, headerSize, currentSegment.getBaseOffset()));
        }
    }
}
