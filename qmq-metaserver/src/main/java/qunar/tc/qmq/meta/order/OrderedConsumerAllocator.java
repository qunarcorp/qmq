package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.PartitionAllocation;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public interface OrderedConsumerAllocator {

    PartitionAllocation allocate(String subject, String group, String clientId);
}
