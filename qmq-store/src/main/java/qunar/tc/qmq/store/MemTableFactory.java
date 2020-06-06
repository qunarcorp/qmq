package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 2020/6/6
 */
public interface MemTableFactory {
    MemTable create(final long tabletId, final long beginOffset, final int capacity);
}
