package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionSet;

import java.util.List;

/**
 * partition 分配策略
 *
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public interface PartitionAllocationStrategy {

    PartitionAllocation allocate(PartitionSet partitionSet, List<String> onlineConsumerList, String consumerGroup);
}
