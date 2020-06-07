package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 2020/6/6
 */
public abstract class MemTable {
    private final long tabletId;
    private final long beginOffset;
    private final int capacity;

    private volatile long endOffset;

    public MemTable(final long tabletId, final long beginOffset, final int capacity) {

        this.tabletId = tabletId;
        this.beginOffset = beginOffset;
        this.capacity = capacity;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBeginOffset() {
        return beginOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public abstract int getTotalDataSize();

    public int getCapacity() {
        return capacity;
    }

    public abstract boolean checkWritable(final int writeBytes);

    public abstract void close();
}
