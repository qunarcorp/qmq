package qunar.tc.qmq.meta.order;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import qunar.tc.qmq.meta.PartitionInfo;
import qunar.tc.qmq.meta.store.PartitionStore;
import qunar.tc.qmq.meta.store.impl.PartitionStoreImpl;

import java.util.List;
import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class DefaultOrderedMessageService implements OrderedMessageService {

    private PartitionStore partitionStore = new PartitionStoreImpl();
    private PartitionMapper partitionMapper = new AveragePartitionMapper();

    @Override
    public PartitionInfo registerOrderedMessage(String subject, int physicalPartitionNum) {
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setSubject(subject);
        partitionInfo.setVersion(0);

        int logicalPartitionNum = OrderedMessageConfig.getDefaultLogicalPartitionNum();
        int defaultPhysicalPartitionNum = OrderedMessageConfig.getDefaultPhysicalPartitionNum();
        physicalPartitionNum = physicalPartitionNum == 0 ? defaultPhysicalPartitionNum : physicalPartitionNum;
        List<String> brokerGroups = OrderedMessageConfig.getBrokerGroups();
        List<String> delayBrokerGroups = OrderedMessageConfig.getDelayBrokerGroups();

        // logical => physical mapping
        RangeMap<Integer, Integer> logical2PhysicalPartitionMap = partitionMapper.map(logicalPartitionNum, physicalPartitionNum);

        // physical => brokerGroup mapping
        RangeMap<Integer, Integer> physical2BrokerGroup = partitionMapper.map(physicalPartitionNum, brokerGroups.size());

        // physical => delayBrokerGroup mapping
        RangeMap<Integer, Integer> physical2DelayBrokerGroup = partitionMapper.map(physicalPartitionNum, delayBrokerGroups.size());

        partitionInfo.setLogicalPartitionNum(logicalPartitionNum);
        partitionInfo.setLogical2PhysicalPartition(logical2PhysicalPartitionMap);
        partitionInfo.setPhysicalPartition2Broker(transform(physical2BrokerGroup, brokerGroups));
        partitionInfo.setPhysicalPartition2DelayBroker(transform(physical2DelayBrokerGroup, delayBrokerGroups));

        partitionStore.save(partitionInfo);

        return partitionInfo;
    }

    public <V> RangeMap<Integer, V> transform(RangeMap<Integer, Integer> source, List<V> replacer) {
        RangeMap<Integer, V> result = TreeRangeMap.create();
        Map<Range<Integer>, Integer> rangeVMap = source.asMapOfRanges();
        for (Map.Entry<Range<Integer>, Integer> rangeVEntry : rangeVMap.entrySet()) {
            result.put(rangeVEntry.getKey(), replacer.get(rangeVEntry.getValue()));
        }
        return result;
    }
}
