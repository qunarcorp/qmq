package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import org.springframework.transaction.support.TransactionTemplate;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.store.PartitionSetStore;
import qunar.tc.qmq.meta.store.PartitionStore;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.store.impl.DatabaseStore;
import qunar.tc.qmq.meta.store.impl.PartitionSetStoreImpl;
import qunar.tc.qmq.meta.store.impl.PartitionStoreImpl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class DefaultOrderedMessageService implements OrderedMessageService {

    private Store store = new DatabaseStore();
    private PartitionStore partitionStore = new PartitionStoreImpl();
    private PartitionSetStore partitionSetStore = new PartitionSetStoreImpl();
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

    public <V> Map<Integer, V> transform(Map<Integer, Integer> source, List<V> replacer) {
        Map<Integer, V> result = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> rangeVEntry : source.entrySet()) {
            result.put(rangeVEntry.getKey(), replacer.get(rangeVEntry.getValue()));
        }
        return result;
    }
}
