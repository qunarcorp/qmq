package qunar.tc.qmq.config;

import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public interface OrderedMessageManager {

    PartitionMapping getPartitionMapping(String subject);

    PartitionAllocation getPartitionAllocation(String subject, String group);
}
