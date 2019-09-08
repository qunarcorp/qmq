package qunar.tc.qmq.broker;

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumerAllocation;
import qunar.tc.qmq.meta.PartitionMapping;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public interface OrderedMessageManager {

    PartitionMapping getPartitionMapping(ClientType clientType, String subject);

    ConsumerAllocation getConsumerAllocation(String subject, String group, String clientId);
}
