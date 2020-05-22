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
import qunar.tc.qmq.store.buffer.SegmentBuffer;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractLogVisitor<Record> implements LogVisitor<Record>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractLogVisitor.class);

    private final LogSegment currentSegment;
    private final SegmentBuffer currentBuffer;
    private final AtomicInteger visitedBufferSize = new AtomicInteger(0);
    private final long startOffset;

    public AbstractLogVisitor(final LogManager logManager, final long startOffset) {
        final long initialOffset = initialOffset(logManager, startOffset);
        this.currentSegment = logManager.locateSegment(initialOffset);
        this.currentBuffer = initBuffer(initialOffset);
        this.startOffset = initialOffset;
    }

    private long initialOffset(final LogManager logManager, final long originStart) {
        if (originStart < logManager.getMinOffset()) {
            LOG.error("initial log visitor offset less than min offset. start: {}, min: {}", originStart, logManager.getMinOffset());
            return logManager.getMinOffset();
        }
        return originStart;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public LogVisitorRecord<Record> nextRecord() {
        if (currentBuffer == null) {
            return LogVisitorRecord.noMore();
        }

        return readOneRecord(currentBuffer);
    }

    @Override
    public int visitedBufferSize() {
        if (currentBuffer == null) {
            return 0;
        }
        return visitedBufferSize.get();
    }

    protected int getBufferSize() {
        return currentBuffer.getSize();
    }

    private SegmentBuffer initBuffer(final long startOffset) {
        if (currentSegment == null) return null;
        final int pos = (int) (startOffset % currentSegment.getFileSize());
        final SegmentBuffer segmentBuffer = currentSegment.selectSegmentBuffer(pos);
        if (segmentBuffer == null) return null;
        if (segmentBuffer.retain()) return segmentBuffer;
        return null;
    }

    @Override
    public void close() {
        if (currentBuffer == null) return;
        currentBuffer.release();
    }

    protected void setVisitedBufferSize(int size) {
        visitedBufferSize.set(size);
    }

    protected void incrVisitedBufferSize(int delta) {
        visitedBufferSize.addAndGet(delta);
    }

    protected long getBaseOffset() {
        return currentSegment.getBaseOffset();
    }

    protected abstract LogVisitorRecord<Record> readOneRecord(SegmentBuffer buffer);
}
