package qunar.tc.qmq.meta.loadbalance;

import com.google.common.collect.Lists;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author zhenwei.liu
 * @since 2019-09-04
 */
public class OrderedLoadBalance extends AbstractLoadBalance<String> {

    private CachedMetaInfoManager cachedMetaInfoManager;

    public OrderedLoadBalance(CachedMetaInfoManager cachedMetaInfoManager) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
    }

    @Override
    List<String> doSelect(String subject, List<String> brokerGroups, int minNum) {
        // 无视传入的 broker, 按照配置的直接返回
        PartitionMapping partitionMapping = cachedMetaInfoManager.getProducerAllocation(subject);
        if (partitionMapping == null) {
            throw new IllegalArgumentException(String.format("subject %s 无法获取 partition 信息", subject));
        }
        Collection<Partition> partitions = partitionMapping.getLogical2PhysicalPartition().asMapOfRanges().values();
        Set<String> brokerGroupSet = partitions.stream().map(Partition::getBrokerGroup).collect(Collectors.toSet());
        return Lists.newArrayList(brokerGroupSet);
    }
}
