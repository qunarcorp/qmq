package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.PartitionAllocation;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public interface PartitionAllocationStore {

    int save(PartitionAllocation partitionAllocation);
    int update(PartitionAllocation partitionAllocation, int oldVersion);

    PartitionAllocation getLatest(String subject, String group);
    List<PartitionAllocation> getLatest();
}
