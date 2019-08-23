package qunar.tc.qmq.meta.order;

import com.google.common.collect.Maps;
import com.google.common.collect.RangeMap;
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
    private RangePartitionMapper rangePartitionMapper = new AverageRangePartitionMapper();
    private PartitionMapper partitionMapper = new AveragePartitionMapper();

    @Override
    public PartitionInfo registerOrderedMessage(String subject, int physicalPartitionNum) {

        // TODO(zhenwei.liu) 这里要加一个主题是否上线的判断, 已经上线的主题不能转化为顺序消息
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setSubject(subject);
        partitionInfo.setVersion(0);

        int logicalPartitionNum = OrderedMessageConfig.getDefaultLogicalPartitionNum();
        int defaultPhysicalPartitionNum = OrderedMessageConfig.getDefaultPhysicalPartitionNum();
        physicalPartitionNum = physicalPartitionNum == 0 ? defaultPhysicalPartitionNum : physicalPartitionNum;
        List<String> brokerGroups = OrderedMessageConfig.getBrokerGroups();
        List<String> delayBrokerGroups = OrderedMessageConfig.getDelayBrokerGroups();

        // logical => physical mapping
        RangeMap<Integer, Integer> logical2PhysicalPartitionMap = rangePartitionMapper.map(logicalPartitionNum, physicalPartitionNum);

        // physical => brokerGroup mapping
        Map<Integer, Integer> physical2BrokerGroup = partitionMapper.map(physicalPartitionNum, brokerGroups.size());

        // physical => delayBrokerGroup mapping
        Map<Integer, Integer> physical2DelayBrokerGroup = partitionMapper.map(physicalPartitionNum, delayBrokerGroups.size());

        partitionInfo.setLogicalPartitionNum(logicalPartitionNum);
        partitionInfo.setLogical2PhysicalPartition(logical2PhysicalPartitionMap);
        partitionInfo.setPhysicalPartition2Broker(transform(physical2BrokerGroup, brokerGroups));
        partitionInfo.setPhysicalPartition2DelayBroker(transform(physical2DelayBrokerGroup, delayBrokerGroups));

        partitionStore.save(partitionInfo);

        return partitionInfo;
    }

    public <V> Map<Integer, V> transform(Map<Integer, Integer> source, List<V> replacer) {
        Map<Integer, V> result = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> rangeVEntry : source.entrySet()) {
            result.put(rangeVEntry.getKey(), replacer.get(rangeVEntry.getValue()));
        }
        return result;
    }
}
