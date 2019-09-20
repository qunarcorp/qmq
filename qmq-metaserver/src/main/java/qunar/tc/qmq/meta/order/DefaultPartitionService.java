package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionTemplate;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerGroupKind;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.*;
import qunar.tc.qmq.meta.store.impl.*;

import java.util.*;
import java.util.stream.Collectors;

import static qunar.tc.qmq.common.PartitionConstants.EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class DefaultPartitionService implements PartitionService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPartitionService.class);

    private PartitionNameResolver partitionNameResolver = new DefaultPartitionNameResolver();
    private Store store = new DatabaseStore();
    private ClientMetaInfoStore clientMetaInfoStore = new ClientMetaInfoStoreImpl();
    private PartitionStore partitionStore = new PartitionStoreImpl();
    private PartitionSetStore partitionSetStore = new PartitionSetStoreImpl();
    private PartitionAllocationStore partitionAllocationStore = new PartitionAllocationStoreImpl();
    private PartitionAllocator partitionAllocator = new AveragePartitionAllocator();
    private TransactionTemplate transactionTemplate = JdbcTemplateHolder.getTransactionTemplate();
    private RangeMapper rangeMapper = new AverageRangeMapper();
    private ItemMapper itemMapper = new AverageItemMapper();

    private static DefaultPartitionService instance = new DefaultPartitionService();

    public static DefaultPartitionService getInstance() {
        return instance;
    }

    private DefaultPartitionService() {
    }

    @Override
    public boolean registerOrderedMessage(String subject, int physicalPartitionNum) {
        // TODO(zhenwei.liu) 这里需要重新分配 subject-broker 表的数据
        synchronized (subject.intern()) {
            PartitionSet oldPartitionSet = partitionSetStore.getLatest(subject);
            if (oldPartitionSet == null) {
                synchronized (subject.intern()) {
                    int logicalPartitionNum = PartitionConstants.DEFAULT_LOGICAL_PARTITION_NUM;
                    int defaultPhysicalPartitionNum = PartitionConstants.DEFAULT_PHYSICAL_PARTITION_NUM;
                    physicalPartitionNum = physicalPartitionNum == 0 ? defaultPhysicalPartitionNum : physicalPartitionNum;
                    List<String> brokerGroups = store.getAllBrokerGroups().stream()
                            .filter(brokerGroup -> Objects.equals(brokerGroup.getKind(), BrokerGroupKind.NORMAL))
                            .map(BrokerGroup::getGroupName)
                            .collect(Collectors.toList());

                    // 初始化 physical/logical partition 列表
                    List<Integer> physicalPartitionList = createIntList(physicalPartitionNum);
                    List<Integer> logicalPartitionList = createIntList(logicalPartitionNum);

                    // logical => physical mapping
                    Map<Integer, List<Integer>> physical2LogicalPartitionMap = rangeMapper.map(physicalPartitionList, logicalPartitionList);

                    // physical => brokerGroup mapping
                    Map<Integer, String> physical2BrokerGroupMap = itemMapper.map(physicalPartitionList, brokerGroups);

                    List<Partition> partitions = Lists.newArrayList();
                    for (Map.Entry<Integer, List<Integer>> entry : physical2LogicalPartitionMap.entrySet()) {
                        Integer physicalPartition = entry.getKey();
                        List<Integer> logicalPartitions = entry.getValue();
                        Range<Integer> logicalPartitionRange =
                                Range.closedOpen(logicalPartitions.get(0), logicalPartitions.get(logicalPartitions.size() - 1) + 1);

                        String partitionName = partitionNameResolver.getPartitionName(subject, physicalPartition);
                        Partition partition = new Partition(
                                subject,
                                partitionName,
                                physicalPartition,
                                logicalPartitionRange,
                                physical2BrokerGroupMap.get(physicalPartition),
                                Partition.Status.RW);

                        partitions.add(partition);
                    }

                    PartitionSet partitionSet = new PartitionSet();
                    partitionSet.setSubject(subject);
                    partitionSet.setVersion(0);
                    partitionSet.setPhysicalPartitions(partitions.stream().map(Partition::getPartitionId).collect(Collectors.toSet()));

                    return transactionTemplate.execute(transactionStatus -> {
                        try {
                            partitionStore.save(partitions);
                            partitionSetStore.save(partitionSet);
                            return true;
                        } catch (DuplicateKeyException e) {
                            // 并发问题, 忽略
                            logger.warn("subject {} 创建分区信息重复", subject);
                            return false;
                        }
                    });
                }
            }
        }
        return false;
    }

    @Override
    public PartitionSet getLatestPartitionSet(String subject) {
        return partitionSetStore.getLatest(subject);
    }

    @Override
    public List<PartitionSet> getLatestPartitionSets() {
        return partitionSetStore.getLatest();
    }

    @Override
    public List<Partition> getAllPartitions() {
        return partitionStore.getAll();
    }

    @Override
    public List<PartitionAllocation> getLatestPartitionAllocations() {
        return partitionAllocationStore.getLatest();
    }

    @Override
    public PartitionAllocation allocatePartitions(PartitionSet partitionSet, List<String> onlineConsumerList, String consumerGroup) {
        String subject = partitionSet.getSubject();
        List<Partition> partitions = partitionStore.getByPartitionIds(subject, partitionSet.getPhysicalPartitions());
        Map<Integer, Partition> partitionMap = partitions.stream().collect(Collectors.toMap(Partition::getPartitionId, p -> p));
        return partitionAllocator.allocate(partitionSet, partitionMap, onlineConsumerList, consumerGroup);
    }

    @Override
    public boolean updatePartitionAllocation(PartitionAllocation newAllocation, int baseVersion) {
        if (baseVersion == -1) {
            try {
                return partitionAllocationStore.save(newAllocation) > 0;
            } catch (DuplicateKeyException e) {
                // 并发问题, 不更新就好
                return false;
            }
        } else {
            return partitionAllocationStore.update(newAllocation, baseVersion) > 0;
        }
    }

    @Override
    public List<ClientMetaInfo> getOnlineExclusiveConsumers() {
        Date updateTime = new Date(System.currentTimeMillis() - EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS);
        return clientMetaInfoStore.queryClientsUpdateAfterDate(ClientType.CONSUMER, OnOfflineState.ONLINE, updateTime);
    }

    @Override
    public List<ClientMetaInfo> getOnlineExclusiveConsumers(String subject, String consumerGroup) {
        Date updateTime = new Date(System.currentTimeMillis() - EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS);
        return clientMetaInfoStore.queryClientsUpdateAfterDate(subject, consumerGroup, ClientType.CONSUMER, OnOfflineState.ONLINE, updateTime);
    }

    private List<Integer> createIntList(int size) {
        ArrayList<Integer> intList = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            intList.add(i);
        }
        return intList;
    }
}
