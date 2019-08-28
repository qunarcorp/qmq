package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.PartitionAllocation;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public interface PartitionAllocationStore {

    int save(PartitionAllocation partitionAllocation);

    List<PartitionAllocation> getLatest();
}
