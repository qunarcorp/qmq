package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.meta.PartitionAllocation;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class DefaultOrderedConsumerAllocator implements OrderedConsumerAllocator {

    @Override
    public PartitionAllocation allocate(String subject, String group, String clientId) {
        return null;
    }
}
