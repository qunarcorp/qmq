package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.meta.ProducerAllocation;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-20
 */
public interface AllocationService {

    ProducerAllocation getProducerAllocation(ClientType clientType, String subject, List<BrokerGroup> defaultBrokerGroups);

    ConsumerAllocation getConsumerAllocation(String subject, String consumerGroup, String clientId, ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups);
}
