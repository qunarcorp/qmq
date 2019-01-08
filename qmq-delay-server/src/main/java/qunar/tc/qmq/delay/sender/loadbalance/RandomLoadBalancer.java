package qunar.tc.qmq.delay.sender.loadbalance;

import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-01-08 17:30
 */
public class RandomLoadBalancer implements LoadBalancer {

    RandomLoadBalancer() {
    }

    @Override
    public BrokerGroupInfo select(BrokerClusterInfo clusterInfo) {
        List<BrokerGroupInfo> groups = clusterInfo.getGroups();
        int size = groups.size();
        if (size == 0) return null;
        BrokerGroupInfo brokerGroupInfo;
        for (int i = 0; i < size * 3; ++i) {
            int index = randomInt(size);
            brokerGroupInfo = groups.get(index);
            if (brokerGroupInfo.isAvailable()) {
                return brokerGroupInfo;
            }
        }

        return null;
    }

    @Override
    public BrokerGroupStats getBrokerGroupStats(BrokerGroupInfo brokerGroupInfo) {
        throw new RuntimeException("UnsupportedOps");
    }

    private int randomInt(int count) {
        return ThreadLocalRandom.current().nextInt(count);
    }

}
