package qunar.tc.qmq.broker;

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.meta.ProducerAllocation;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public interface ClientMetaManager {

    ProducerAllocation getProducerAllocation(ClientType clientType, String subject);
}
