package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionSet;

import java.util.List;
import java.util.Map;

/**
 * partition 分配策略
 *
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public interface PartitionAllocator {

    PartitionAllocation allocate(PartitionSet partitionSet, Map<Integer, Partition> partitionMap, List<String> onlineConsumerList, String consumerGroup);
}
