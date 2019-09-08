package qunar.tc.qmq.broker;

import qunar.tc.qmq.ProduceMessage;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public interface OrderStrategy {

    OrderStrategy STRICT = new StrictOrderStrategy();
    OrderStrategy BEST_TRIED = new BestTriedOrderStrategy();

    BrokerGroupInfo resolveBrokerGroup(String currentBrokerGroup, BrokerClusterInfo brokerCluster);

    boolean needRetry(ProduceMessage message);

    String name();

    class StrictOrderStrategy implements OrderStrategy {

        @Override
        public BrokerGroupInfo resolveBrokerGroup(String brokerGroupName, BrokerClusterInfo brokerCluster) {
            return brokerCluster.getGroupByName(brokerGroupName);
        }

        @Override
        public boolean needRetry(ProduceMessage message) {
            return true;
        }

        @Override
        public String name() {
            return "STRICT";
        }
    }

    class BestTriedOrderStrategy implements OrderStrategy {

        @Override
        public BrokerGroupInfo resolveBrokerGroup(String brokerGroupName, BrokerClusterInfo brokerCluster) {
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

        @Override
        public boolean needRetry(ProduceMessage message) {
            return message.getTries() < message.getMaxTries();
        }

        @Override
        public String name() {
            return "BEST_TRIED";
        }
    }
}
