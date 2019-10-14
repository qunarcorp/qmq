package qunar.tc.qmq.meta.order;

import static qunar.tc.qmq.common.PartitionConstants.EMPTY_VERSION;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.collect.TreeRangeMap;
import com.google.common.primitives.Ints;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;

/**
 * @author zhenwei.liu
 * @since 2019-09-20
 */
public class DefaultAllocationService implements AllocationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAllocationService.class);

    private CachedMetaInfoManager cachedMetaInfoManager;
    private ConsumerPartitionAllocator consumerPartitionAllocator;
    private PartitionService partitionService;
    private PartitionNameResolver partitionNameResolver;

    public DefaultAllocationService(CachedMetaInfoManager cachedMetaInfoManager,
            ConsumerPartitionAllocator consumerPartitionAllocator, PartitionService partitionService) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.consumerPartitionAllocator = consumerPartitionAllocator;
        this.partitionService = partitionService;
        this.partitionNameResolver = new OldPartitionNameResolver();
    }

    @Override
    public ProducerAllocation getProducerAllocation(ClientType clientType, String subject,
            List<BrokerGroup> brokerGroups) {
        PartitionSet partitionSet = cachedMetaInfoManager.getLatestPartitionSet(subject);

        if (partitionSet == null) {
            if (Objects.equals(clientType, ClientType.DELAY_PRODUCER)) {
                return getDefaultProducerAllocation(subject, brokerGroups);
            } else {
                // 初始化 partition set
                List<Partition> partitions = partitionService.mapPartitions(subject, brokerGroups.size(),
                        brokerGroups.stream().map(BrokerGroup::getGroupName).collect(Collectors.toList()), null,
                        partitionNameResolver);
                partitionSet = partitionService.mapPartitionSet(subject, partitions, null);
                partitionService.updatePartitions(subject, partitionSet, partitions, null);
                partitionSet = cachedMetaInfoManager.getLatestPartitionSet(subject);
            }
        }

        Set<Integer> partitionIds = partitionSet.getPhysicalPartitions();
        int version = partitionSet.getVersion();
        RangeMap<Integer, PartitionProps> rangeMap = TreeRangeMap.create();
        for (Integer partitionId : partitionIds) {
            Partition partition = cachedMetaInfoManager.getPartition(subject, partitionId);
            if (partition != null) {
                rangeMap.put(partition.getLogicalPartition(),
                        new PartitionProps(partitionId, partition.getPartitionName(), partition.getBrokerGroup()));
            } else {
                LOGGER.warn("无法找到 Partition subject {} id {}", subject, partitionId);
            }
        }
        return new ProducerAllocation(subject, version, rangeMap);
    }

    private ProducerAllocation getDefaultProducerAllocation(String subject, List<BrokerGroup> brokerGroups) {
        int version = -1;
        int logicalPartitionNum = PartitionConstants.DEFAULT_LOGICAL_PARTITION_NUM;
        int step = (int) Math.ceil((double) logicalPartitionNum / brokerGroups.size());
        TreeRangeMap<Integer, PartitionProps> logicalRangeMap = TreeRangeMap.create();
        int startRange = 0;
        for (BrokerGroup brokerGroup : brokerGroups) {
            int endRange = startRange + step;
            if (endRange > logicalPartitionNum) {
                endRange = logicalPartitionNum;
            }
            Range<Integer> logicalRange = Range.closedOpen(startRange, endRange);
            startRange = endRange;
            logicalRangeMap.put(logicalRange,
                    new PartitionProps(PartitionConstants.EMPTY_PARTITION_ID, subject, brokerGroup.getGroupName()));
        }
        return new ProducerAllocation(subject, version, logicalRangeMap);
    }

    @Override
    public ConsumerAllocation getConsumerAllocation(String subject, String consumerGroup, String clientId,
            long authExpireTime, ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups) {
        if (CollectionUtils.isEmpty(brokerGroups)) {
            return new ConsumerAllocation(-1, Collections.emptyList(), -1, consumeStrategy);
        }

        if (consumeStrategy == ConsumeStrategy.EXCLUSIVE) {
            PartitionAllocation partitionAllocation = cachedMetaInfoManager
                    .getPartitionAllocation(subject, consumerGroup);
            if (partitionAllocation == null) {
                return getDefaultConsumerAllocation(subject, authExpireTime, consumeStrategy, brokerGroups);
            } else {
                return createConsumerAllocation(subject, consumerGroup, clientId, authExpireTime, consumeStrategy,
                        brokerGroups, partitionAllocation);
            }
        } else {
            return getDefaultConsumerAllocation(subject, authExpireTime, consumeStrategy, brokerGroups);
        }
    }

    private ConsumerAllocation createConsumerAllocation(String subject, String consumerGroup, String clientId,
            long authExpireTime, ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups,
            PartitionAllocation partitionAllocation) {
        int version = partitionAllocation.getVersion();
        Map<String, Set<PartitionProps>> clientId2PartitionProps = partitionAllocation.getAllocationDetail()
                .getClientId2PartitionProps();
        Set<PartitionProps> partitionProps = clientId2PartitionProps.get(clientId);
        if (partitionProps == null) {
            // 说明还未给该 client 分配, 应该返回空列表
            partitionProps = Collections.emptySet();
        }

        // 将旧的分区合并到所有 consumer 的分区分配列表
        Set<Integer> latestPartitionIds = partitionProps.stream().map(PartitionProps::getPartitionId)
                .collect(Collectors.toSet());
        List<PartitionSet> allPartitionSets = cachedMetaInfoManager.getPartitionSets(subject);

        List<PartitionProps> mergedPartitionProps = Lists.newArrayList();
        mergedPartitionProps.addAll(partitionProps);

        for (PartitionSet oldPartitionSet : allPartitionSets) {
            if (oldPartitionSet.getVersion() >= partitionAllocation.getPartitionSetVersion()) {
                continue;
            }
            Set<Integer> pps = oldPartitionSet.getPhysicalPartitions();
            SetView<Integer> sharedPartitionIds = Sets.difference(pps, latestPartitionIds);
            if (!CollectionUtils.isEmpty(sharedPartitionIds)) {
                for (Integer sharedPartitionId : sharedPartitionIds) {
                    Partition partition = cachedMetaInfoManager.getPartition(subject, sharedPartitionId);
                    mergedPartitionProps
                            .add(new PartitionProps(partition.getPartitionId(), partition.getPartitionName(),
                                    partition.getBrokerGroup()));
                }
            }
        }

        mergedPartitionProps.sort((o1, o2) -> Ints.compare(o1.getPartitionId(), o2.getPartitionId()));

        return new ConsumerAllocation(version, mergedPartitionProps, authExpireTime, consumeStrategy);
    }

    private ConsumerAllocation getDefaultConsumerAllocation(String subject, long authExpireTime,
            ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups) {
        List<PartitionProps> partitionProps = Lists.newArrayList();
        for (BrokerGroup brokerGroup : brokerGroups) {
            partitionProps.add(new PartitionProps(PartitionConstants.EMPTY_PARTITION_ID, subject,
                    brokerGroup.getGroupName()));
        }
        return new ConsumerAllocation(EMPTY_VERSION, partitionProps, authExpireTime, consumeStrategy);
    }
}
