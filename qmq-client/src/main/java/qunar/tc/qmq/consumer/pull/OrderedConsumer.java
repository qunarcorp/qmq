package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.meta.PartitionAllocation;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface OrderedConsumer {

    PartitionAllocation getPartitionAllocation();
}
