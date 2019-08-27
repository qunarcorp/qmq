package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.order.Partition;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface PartitionStore {

    int save(Partition partition);

    int save(List<Partition> partitions);

    List<Partition> getByPartitionIds(List<Integer> partitionIds);
}
