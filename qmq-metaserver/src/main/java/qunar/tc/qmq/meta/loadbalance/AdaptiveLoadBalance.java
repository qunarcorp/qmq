package qunar.tc.qmq.meta.loadbalance;

import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-04
 */
public class AdaptiveLoadBalance implements LoadBalance<String> {

    private CachedMetaInfoManager cachedMetaInfoManager;
    private RandomLoadBalance<String> defaultLoadBalance;
    private OrderedLoadBalance orderedLoadBalance;

    public AdaptiveLoadBalance(CachedMetaInfoManager cachedMetaInfoManager, RandomLoadBalance<String> defaultLoadBalance, OrderedLoadBalance orderedLoadBalance) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.defaultLoadBalance = defaultLoadBalance;
        this.orderedLoadBalance = orderedLoadBalance;
    }

    @Override
    public List<String> select(String subject, List<String> brokerGroups, int minNum) {
        // TODO(zhenwei.liu) 这里应该不需要判断是否顺序了, 是否应该全部返回 broker 列表
        PartitionMapping partitionMapping = cachedMetaInfoManager.getProducerAllocation(subject);
        if (partitionMapping == null) {
            return defaultLoadBalance.select(subject, brokerGroups, minNum);
        } else {
            return orderedLoadBalance.select(subject, brokerGroups, minNum);
        }
    }
}
