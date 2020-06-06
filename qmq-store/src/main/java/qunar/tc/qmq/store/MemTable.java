package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 2020/6/6
 */
public abstract class MemTable {
    private final long tabletId;
    private final long beginOffset;
    private final int capacity;

    public MemTable(final long tabletId, final long beginOffset, final int capacity){

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

    public abstract void close();
}
