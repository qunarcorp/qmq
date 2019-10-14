package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class AveragePartitionAllocateStrategy implements PartitionAllocateStrategy {

    private ItemMapper itemMapper = new AverageItemMapper();

    @Override
    public PartitionAllocation allocate(PartitionSet partitionSet, Map<Integer, Partition> partitionMap, List<String> onlineConsumerList, String consumerGroup) {
        Set<Integer> partitionIds = Sets.newTreeSet(partitionSet.getPhysicalPartitions());
        // partition => client
        Map<Integer, String> partitionClientMapping = itemMapper.map(Lists.newArrayList(partitionIds), onlineConsumerList);
        // clientId => subjectLocations
        Map<String, Set<PartitionProps>> clientId2SubjectLocations = Maps.newHashMap();

        // client => partitionSet
        for (Map.Entry<Integer, String> entry : partitionClientMapping.entrySet()) {
            Integer partitionId = entry.getKey();
            Partition partition = partitionMap.get(partitionId);
            String clientId = entry.getValue();
            Set<PartitionProps> partitionPropsSet = clientId2SubjectLocations.computeIfAbsent(clientId, k -> Sets.newHashSet());
            partitionPropsSet.add(new PartitionProps(partitionId, partition.getPartitionName(), partition.getBrokerGroup()));
        }

        // 对于未被分配到分区的 client, 需要给一个空占位符
        for (String consumerId : onlineConsumerList) {
            clientId2SubjectLocations.computeIfAbsent(consumerId, k -> Sets.newHashSet());
        }

        PartitionAllocation.AllocationDetail allocationDetail = new PartitionAllocation.AllocationDetail();
        allocationDetail.setClientId2PartitionProps(clientId2SubjectLocations);

        PartitionAllocation partitionAllocation = new PartitionAllocation();
        partitionAllocation.setSubject(partitionSet.getSubject());
        partitionAllocation.setConsumerGroup(consumerGroup);
        partitionAllocation.setAllocationDetail(allocationDetail);
        partitionAllocation.setPartitionSetVersion(partitionSet.getVersion());

        return partitionAllocation;
    }
}
