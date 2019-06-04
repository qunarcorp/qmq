package qunar.tc.qmq.store;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-28 16:23
 */
public class IndexCheckpoint {
    private long msgOffset;
    private long indexOffset;

    public IndexCheckpoint(long msgOffset, long indexOffset) {
        this.msgOffset = msgOffset;
        this.indexOffset = indexOffset;
    }

    public long getMsgOffset() {
        return msgOffset;
    }

    public void setMsgOffset(long msgOffset) {
        this.msgOffset = msgOffset;
    }

    public long getIndexOffset() {
        return indexOffset;
    }

    public void setIndexOffset(long indexOffset) {
        this.indexOffset = indexOffset;
    }
}
