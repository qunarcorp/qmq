package qunar.tc.qmq.delay.sender.loadbalance;

import com.google.common.cache.*;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-01-08 15:12
 */
class LoadBalanceStats {
    private final LoadingCache<BrokerGroupInfo, BrokerGroupStats> brokerGroupStatsCache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES).build(new CacheLoader<BrokerGroupInfo, BrokerGroupStats>() {
                @Override
                public BrokerGroupStats load(BrokerGroupInfo key) throws Exception {
                    return createBrokerGroupStats(key);
                }
            });

    private BrokerGroupStats createBrokerGroupStats(BrokerGroupInfo brokerGroupInfo) {
        return new BrokerGroupStats(brokerGroupInfo);
    }

    BrokerGroupStats getBrokerGroupStats(final BrokerGroupInfo brokerGroup) {
        try {
            return brokerGroupStatsCache.get(brokerGroup);
        } catch (ExecutionException e) {
            BrokerGroupStats stats = createBrokerGroupStats(brokerGroup);
            brokerGroupStatsCache.asMap().putIfAbsent(brokerGroup, stats);
            return brokerGroupStatsCache.asMap().get(brokerGroup);
        }
    }

}
