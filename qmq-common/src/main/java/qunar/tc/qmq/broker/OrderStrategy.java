package qunar.tc.qmq.broker;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public interface OrderStrategy {

    OrderStrategy STRICT = new StrictOrderStrategy();
    OrderStrategy BEST_TRIED = new BestTriedOrderStategy();

    BrokerGroupInfo getBrokerGroup(String brokerGroupName, BrokerClusterInfo brokerCluster);

    class StrictOrderStrategy implements OrderStrategy {

        @Override
        public BrokerGroupInfo getBrokerGroup(String brokerGroupName, BrokerClusterInfo brokerCluster) {
            return brokerCluster.getGroupByName(brokerGroupName);
        }
    }

    class BestTriedOrderStategy implements OrderStrategy {

        @Override
        public BrokerGroupInfo getBrokerGroup(String brokerGroupName, BrokerClusterInfo brokerCluster) {
            // best tried 策略当分区不可用时, 会使用另一个可用分区
            List<BrokerGroupInfo> groups = brokerCluster.getGroups();
            int brokerGroupIdx = 0;
            BrokerGroupInfo brokerGroup = brokerCluster.getGroupByName(brokerGroupName);
            while (!brokerGroup.isAvailable() && brokerGroupIdx < groups.size()) {
                brokerGroup = groups.get(brokerGroupIdx);
                brokerGroupIdx++;
            }
            return brokerGroup;
        }
    }
}
