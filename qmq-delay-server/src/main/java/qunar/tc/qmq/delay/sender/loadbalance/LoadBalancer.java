package qunar.tc.qmq.delay.sender.loadbalance;

import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-01-08 14:46
 */
public interface LoadBalancer {
    BrokerGroupInfo select(BrokerClusterInfo clusterInfo);

    BrokerGroupStats getBrokerGroupStats(BrokerGroupInfo brokerGroupInfo);
}
