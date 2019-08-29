package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class AveragePartitionAllocationStrategy implements PartitionAllocationStrategy {

    private ItemMapper itemMapper = new AverageItemMapper();

    @Override
    public PartitionAllocation allocate(PartitionSet partitionSet, List<String> onlineConsumerList, String consumerGroup) {
        Set<Integer> physicalPartitions = Sets.newTreeSet(partitionSet.getPhysicalPartitions());
        // partition => client
        Map<Integer, String> partitionClientMapping = itemMapper.map(Lists.newArrayList(physicalPartitions), onlineConsumerList);
        Map<String, Set<Integer>> clientPartitionMapping = Maps.newHashMap();

        // client => partitionSet
        for (Map.Entry<Integer, String> entry : partitionClientMapping.entrySet()) {
            Integer partitionId = entry.getKey();
            String clientId = entry.getValue();
            Set<Integer> partitionIdSet = clientPartitionMapping.computeIfAbsent(clientId, k -> Sets.newHashSet());
            partitionIdSet.add(partitionId);
        }

        PartitionAllocation.AllocationDetail allocationDetail = new PartitionAllocation.AllocationDetail();
        allocationDetail.setClientId2PhysicalPartitions(clientPartitionMapping);

        PartitionAllocation partitionAllocation = new PartitionAllocation();
        partitionAllocation.setSubject(partitionSet.getSubject());
        partitionAllocation.setConsumerGroup(consumerGroup);
        partitionAllocation.setAllocationDetail(allocationDetail);
        partitionAllocation.setPartitionSetVersion(partitionSet.getVersion());

        return partitionAllocation;
    }
}
