package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
