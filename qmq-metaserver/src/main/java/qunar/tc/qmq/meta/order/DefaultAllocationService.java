package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;

import java.util.*;

import static qunar.tc.qmq.common.PartitionConstants.EMPTY_VERSION;
import static qunar.tc.qmq.common.PartitionConstants.EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS;

/**
 * @author zhenwei.liu
 * @since 2019-09-20
 */
public class DefaultAllocationService implements AllocationService {

    private CachedMetaInfoManager cachedMetaInfoManager;

    public DefaultAllocationService(CachedMetaInfoManager cachedMetaInfoManager) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
    }

    @Override
    public ProducerAllocation getProducerAllocation(ClientType clientType, String subject, List<BrokerGroup> defaultBrokerGroups) {
        PartitionSet partitionSet = cachedMetaInfoManager.getPartitionSet(subject);
        if (partitionSet == null || Objects.equals(clientType, ClientType.DELAY_PRODUCER)) {
            return getDefaultProducerAllocation(subject, defaultBrokerGroups);
        } else {
            Set<Integer> partitionIds = partitionSet.getPhysicalPartitions();
            int version = partitionSet.getVersion();
            RangeMap<Integer, PartitionProps> rangeMap = TreeRangeMap.create();
            for (Integer partitionId : partitionIds) {
                Partition partition = cachedMetaInfoManager.getPartition(subject, partitionId);
                rangeMap.put(partition.getLogicalPartition(), new PartitionProps(partitionId, partition.getPartitionName(), partition.getBrokerGroup()));
            }
            return new ProducerAllocation(subject, version, rangeMap);
        }
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
            logicalRangeMap.put(logicalRange, new PartitionProps(PartitionConstants.EMPTY_PARTITION_ID, subject, brokerGroup.getGroupName()));
        }
        return new ProducerAllocation(subject, version, logicalRangeMap);
    }

    @Override
    public ConsumerAllocation getConsumerAllocation(String subject, String consumerGroup, String clientId, ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups) {
        PartitionAllocation partitionAllocation = cachedMetaInfoManager.getPartitionAllocation(subject, consumerGroup);
        if (partitionAllocation != null) {
            int version = partitionAllocation.getVersion();
            Map<String, Set<PartitionProps>> clientId2PartitionProps = partitionAllocation.getAllocationDetail().getClientId2PartitionProps();
            Set<PartitionProps> partitionProps = clientId2PartitionProps.get(clientId);
            if (partitionProps == null) {
                // 说明还未给该 client 分配, 应该返回空列表
                partitionProps = Collections.emptySet();
            }
            return new ConsumerAllocation(version, Lists.newArrayList(partitionProps), expiredTimestamp(), consumeStrategy);
        } else {
            return getDefaultConsumerAllocation(subject, consumeStrategy, brokerGroups);
        }
    }

    private ConsumerAllocation getDefaultConsumerAllocation(String subject, ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups) {
        List<PartitionProps> partitionProps = Lists.newArrayList();
        for (BrokerGroup brokerGroup : brokerGroups) {
            partitionProps.add(new PartitionProps(PartitionConstants.EMPTY_PARTITION_ID, subject, brokerGroup.getGroupName()));
        }
        return new ConsumerAllocation(EMPTY_VERSION, partitionProps, expiredTimestamp(), consumeStrategy);
    }

    private long expiredTimestamp() {
        return System.currentTimeMillis() + EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS;
    }
}
