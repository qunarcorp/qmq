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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class ActionLogVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(ActionLogVisitor.class);

    private static final int MIN_RECORD_BYTES = 5; // 4 bytes magic + 1 byte record type

    private final LogSegment currentSegment;
    private final SegmentBuffer currentBuffer;
    private final AtomicInteger visitedBufferSize = new AtomicInteger(0);
    private final long startOffset;

    public ActionLogVisitor(final LogManager logManager, final long startOffset) {
        final long initialOffset = initialOffset(logManager, startOffset);
        this.currentSegment = logManager.locateSegment(initialOffset);
        this.currentBuffer = selectBuffer(initialOffset);
        this.startOffset = initialOffset;
    }

    private long initialOffset(final LogManager logManager, final long originStart) {
        if (originStart < logManager.getMinOffset()) {
            LOG.error("initial action log visitor offset less than min offset. start: {}, min: {}",
                    originStart, logManager.getMinOffset());
            return logManager.getMinOffset();
        }

        return originStart;
    }

    public Optional<Action> nextAction() {
        if (currentBuffer == null) {
            return null;
        }
        return readAction(currentBuffer.getBuffer());
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

    // TODO(keli.wang): need replace optional with custom class
    private Optional<Action> readAction(final ByteBuffer buffer) {
        if (buffer.remaining() < MIN_RECORD_BYTES) {
            return null;
        }

        final int startPos = buffer.position();
        final int magic = buffer.getInt();
        if (magic != MagicCode.ACTION_LOG_MAGIC_V1) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        }

        final byte recordType = buffer.get();
        if (recordType == 2) {
            if (buffer.remaining() < Integer.BYTES) {
                return null;
            }
            final int blankSize = buffer.getInt();
            visitedBufferSize.addAndGet(blankSize + (buffer.position() - startPos));
            return Optional.empty();
        } else if (recordType == 1) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        } else if (recordType == 0) {
            try {
                if (buffer.remaining() < Integer.BYTES + Byte.BYTES) {
                    return null;
                }
                final ActionType payloadType = ActionType.fromCode(buffer.get());
                final int payloadSize = buffer.getInt();
                if (buffer.remaining() < payloadSize) {
                    return null;
                }
                final Action action = payloadType.getReaderWriter().read(buffer);
                visitedBufferSize.addAndGet(buffer.position() - startPos);
                return Optional.of(action);
            } catch (Exception e) {
                LOG.error("fail read action log", e);
                return null;
            }
        } else {
            throw new RuntimeException("Unknown record type");
        }
    }

    public long getStartOffset() {
        return startOffset;
    }
}
