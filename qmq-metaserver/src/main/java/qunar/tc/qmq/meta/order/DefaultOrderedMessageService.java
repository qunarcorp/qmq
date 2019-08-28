package qunar.tc.qmq.meta.order;

import com.google.common.collect.*;
import org.springframework.transaction.support.TransactionTemplate;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.store.PartitionAllocationStore;
import qunar.tc.qmq.meta.store.PartitionSetStore;
import qunar.tc.qmq.meta.store.PartitionStore;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.store.impl.DatabaseStore;
import qunar.tc.qmq.meta.store.impl.PartitionAllocationStoreImpl;
import qunar.tc.qmq.meta.store.impl.PartitionSetStoreImpl;
import qunar.tc.qmq.meta.store.impl.PartitionStoreImpl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class DefaultOrderedMessageService implements OrderedMessageService {

    private Store store = new DatabaseStore();
    private PartitionStore partitionStore = new PartitionStoreImpl();
    private PartitionSetStore partitionSetStore = new PartitionSetStoreImpl();
    private PartitionAllocationStore partitionAllocationStore = new PartitionAllocationStoreImpl();
    private TransactionTemplate transactionTemplate = JdbcTemplateHolder.getTransactionTemplate();
    private RangePartitionMapper rangePartitionMapper = new AverageRangePartitionMapper();
    private PartitionMapper partitionMapper = new AveragePartitionMapper();

    @Override
    public void registerOrderedMessage(String subject, int physicalPartitionNum) {

        SubjectInfo subjectInfo = store.getSubjectInfo(subject);
        if (subjectInfo != null) {
            throw new IllegalArgumentException(String.format("subject %s 已存在", subject));
        }

        int logicalPartitionNum = OrderedMessageConfig.getDefaultLogicalPartitionNum();
        int defaultPhysicalPartitionNum = OrderedMessageConfig.getDefaultPhysicalPartitionNum();
        physicalPartitionNum = physicalPartitionNum == 0 ? defaultPhysicalPartitionNum : physicalPartitionNum;
        List<String> brokerGroups = OrderedMessageConfig.getBrokerGroups();

        // logical => physical mapping
        RangeMap<Integer, Integer> logical2PhysicalPartitionMap = rangePartitionMapper.map(logicalPartitionNum, physicalPartitionNum);

        // physical => brokerGroup mapping
        Map<Integer, Integer> physical2BrokerGroup = partitionMapper.map(physicalPartitionNum, brokerGroups.size());

        List<Partition> partitions = Lists.newArrayList();
        for (Map.Entry<Range<Integer>, Integer> entry : logical2PhysicalPartitionMap.asMapOfRanges().entrySet()) {
            Range<Integer> logicalPartitionRange = entry.getKey();
            Integer physicalPartition = entry.getValue();

            Partition partition = new Partition();
            partition.setSubject(subject);
            partition.setLogicalPartition(logicalPartitionRange);
            partition.setPhysicalPartition(physicalPartition);
            partition.setStatus(Partition.Status.RW);

            partitions.add(partition);
        }

        for (Map.Entry<Integer, Integer> entry : physical2BrokerGroup.entrySet()) {
            Integer physicalPartition = entry.getKey();
            Integer brokerGroupIdx = entry.getValue();
            String brokerGroup = brokerGroups.get(brokerGroupIdx);
            for (Partition partition : partitions) {
                if (partition.getPhysicalPartition() == physicalPartition) {
                    partition.setBrokerGroup(brokerGroup);
                    break;
                }
            }
        }

        PartitionSet partitionSet = new PartitionSet();
        partitionSet.setSubject(subject);
        partitionSet.setVersion(0);
        partitionSet.setPhysicalPartitions(partitions.stream().map(Partition::getPhysicalPartition).collect(Collectors.toSet()));

        transactionTemplate.execute(transactionStatus -> {
            partitionStore.save(partitions);
            partitionSetStore.save(partitionSet);
            return null;
        });
    }

    @Override
    public List<PartitionAllocation> getActivatedPartitionAllocations() {
        return partitionAllocationStore.getLatest();
    }

    @Override
    public List<PartitionMapping> getLatestPartitionMappings() {
        List<PartitionMapping> result = Lists.newArrayList();
        List<PartitionSet> latestPartitionSets = partitionSetStore.getLatest();

        Set<Integer> partitionIdSet = Sets.newTreeSet();
        latestPartitionSets.forEach(partitionSet -> partitionIdSet.addAll(partitionSet.getPhysicalPartitions()));
        List<Integer> partitionIdList = Lists.newArrayList(partitionIdSet);
        // subject+id => partition
        Map<String, Partition> partitionMap = Maps.newHashMap();
        int start = 0;
        int step = 100;
        while (start < partitionIdList.size()) {
            int end = start + step;
            if (end > partitionIdList.size()) {
                end = partitionIdList.size();
            }
            List<Integer> subList = partitionIdList.subList(start, end);
            List<Partition> partitionList = partitionStore.getByPartitionIds(subList);
            for (Partition partition : partitionList) {
                partitionMap.put(createPartitionKey(partition), partition);
            }
            start += step;
        }

        for (PartitionSet partitionSet : latestPartitionSets) {
            PartitionMapping partitionMapping = new PartitionMapping();
            result.add(partitionMapping);

            partitionMapping.setSubject(partitionSet.getSubject());
            partitionMapping.setVersion(partitionSet.getVersion());

            Set<Integer> physicalPartitions = partitionSet.getPhysicalPartitions();
            RangeMap<Integer, Partition> logical2PhysicalPartition = TreeRangeMap.create();
            int logicalPartitionNum = 0;
            for (Integer physicalPartition : physicalPartitions) {
                String partitionKey = createPartitionKey(partitionSet.getSubject(), physicalPartition);
                Partition partition = partitionMap.get(partitionKey);
                Range<Integer> logicalPartition = partition.getLogicalPartition();
                logicalPartitionNum += (logicalPartition.upperEndpoint() - logicalPartition.lowerEndpoint());
                logical2PhysicalPartition.put(logicalPartition, partition);
            }
            partitionMapping.setLogical2PhysicalPartition(logical2PhysicalPartition);
            partitionMapping.setLogicalPartitionNum(logicalPartitionNum);
        }

        return result;
    }

    private String createPartitionKey(Partition partition) {
        return createPartitionKey(partition.getSubject(), partition.getPhysicalPartition());
    }

    private String createPartitionKey(String subject, int physicalPartition) {
        return subject + ":" + physicalPartition;
    }
}
