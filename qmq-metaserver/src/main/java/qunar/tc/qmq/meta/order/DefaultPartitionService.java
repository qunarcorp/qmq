package qunar.tc.qmq.meta.order;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.*;
import qunar.tc.qmq.meta.store.impl.*;

import java.util.*;
import java.util.stream.Collectors;

import static qunar.tc.qmq.common.PartitionConstants.EMPTY_VERSION;
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
    public ProducerAllocation getDefaultProducerAllocation(String subject, List<BrokerGroup> brokerGroups) {
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

    /**
     * 在还没有自动为 exclusive consumer 分配分区时, 为所有 Consumer 返回所有的分区
     *
     * @param subject         主题
     * @param consumeStrategy Meta 分配的消费策略
     * @param brokerGroups    broker groups
     * @return 分配结果
     */
    @Override
    public ConsumerAllocation getDefaultConsumerAllocation(String subject, ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups) {
        List<PartitionProps> partitionProps = Lists.newArrayList();
        for (BrokerGroup brokerGroup : brokerGroups) {
            partitionProps.add(new PartitionProps(PartitionConstants.EMPTY_PARTITION_ID, subject, brokerGroup.getGroupName()));
        }
        return new ConsumerAllocation(EMPTY_VERSION, partitionProps, expiredTimestamp(), consumeStrategy);
    }

    /**
     * 获取系统分配的 exclusive 分配结果
     *
     * @param subject       主题
     * @param consumerGroup 消费组
     * @param clientId      client id
     * @return
     */
    @Override
    public ConsumerAllocation getConsumerAllocation(String subject, String consumerGroup, ConsumeStrategy consumeStrategy, String clientId) {
        PartitionAllocation partitionAllocation = getActivatedPartitionAllocation(subject, consumerGroup);
        if (partitionAllocation == null) {
            return null;
        }
        PartitionAllocation.AllocationDetail allocationDetail = partitionAllocation.getAllocationDetail();
        Map<String, Set<PartitionProps>> clientId2PartitionName = allocationDetail.getClientId2SubjectLocation();
        Set<PartitionProps> partitionProps = clientId2PartitionName.get(clientId);
        if (CollectionUtils.isEmpty(partitionProps)) {
            return null;
        }

        int version = partitionAllocation.getVersion();
        return new ConsumerAllocation(version, Lists.newArrayList(partitionProps), expiredTimestamp(), consumeStrategy);
    }

    private long expiredTimestamp() {
        return System.currentTimeMillis() + EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS;
    }

    @Override
    public PartitionAllocation getActivatedPartitionAllocation(String subject, String group) {
        return partitionAllocationStore.getLatest(subject, group);
    }

    @Override
    public List<PartitionAllocation> getActivatedPartitionAllocations() {
        return partitionAllocationStore.getLatest();
    }

    @Override
    public List<ProducerAllocation> getLatestProducerAllocations() {
        List<ProducerAllocation> result = Lists.newArrayList();

        List<PartitionSet> latestPartitionSets = partitionSetStore.getLatest();
        List<Partition> partitions = partitionStore.getAll();
        if (CollectionUtils.isEmpty(latestPartitionSets)) {
            return Collections.emptyList();
        }

        // subject => partitions
        Map<String, List<Partition>> partitionMap = Maps.newHashMap();
        for (Partition partition : partitions) {
            String subject = partition.getSubject();
            List<Partition> plist = partitionMap.computeIfAbsent(subject, k -> Lists.newArrayList());
            plist.add(partition);
        }

        for (PartitionSet partitionSet : latestPartitionSets) {
            Set<Integer> partitionIds = partitionSet.getPhysicalPartitions();
            int version = partitionSet.getVersion();
            String subject = partitionSet.getSubject();
            List<Partition> hisPartitions = partitionMap.get(subject);
            RangeMap<Integer, PartitionProps> logicalRangeMap = TreeRangeMap.create();
            for (Partition hisPartition : hisPartitions) {
                int partitionId = hisPartition.getPartitionId();
                String partitionName = hisPartition.getPartitionName();
                String brokerGroup = hisPartition.getBrokerGroup();
                if (partitionIds.contains(partitionId)) {
                    logicalRangeMap.put(hisPartition.getLogicalPartition(), new PartitionProps(partitionId, partitionName, brokerGroup));
                }
            }
            result.add(new ProducerAllocation(subject, version, logicalRangeMap));
        }

        return result;
    }

    @Override
    public ProducerAllocation getLatestProducerAllocation(String subject) {
        PartitionSet partitionSet = partitionSetStore.getLatest(subject);
        List<Partition> partitionList = partitionStore.getByPartitionIds(subject, partitionSet.getPhysicalPartitions());
        Map<String, Partition> partitionMap = partitionList.stream().collect(Collectors.toMap(this::createPartitionName, p -> p));
        return transformProducerAllocation(partitionSet, partitionMap);
    }

    @Override
    public PartitionSet getLatestPartitionSet(String subject) {
        return partitionSetStore.getLatest(subject);
    }

    private ProducerAllocation transformProducerAllocation(PartitionSet partitionSet, Map<String, Partition> partitionMap) {
        String subject = partitionSet.getSubject();
        int version = partitionSet.getVersion();

        Set<Integer> physicalPartitions = partitionSet.getPhysicalPartitions();

        RangeMap<Integer, PartitionProps> logical2SubjectLocation = TreeRangeMap.create();
        for (Integer physicalPartition : physicalPartitions) {
            String partitionName = createPartitionName(partitionSet.getSubject(), physicalPartition);
            Partition partition = partitionMap.get(partitionName);
            Range<Integer> logicalPartition = partition.getLogicalPartition();
            logical2SubjectLocation.put(logicalPartition, new PartitionProps(physicalPartition, partitionName, partition.getBrokerGroup()));
        }
        return new ProducerAllocation(subject, version, logical2SubjectLocation);
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
    public List<ClientMetaInfo> getOnlineOrderedConsumers() {
        Date updateTime = new Date(System.currentTimeMillis() - EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS);
        return clientMetaInfoStore.queryClientsUpdateAfterDate(ClientType.CONSUMER, OnOfflineState.ONLINE, updateTime);
    }

    @Override
    public List<ClientMetaInfo> getOnlineOrderedConsumers(String subject, String consumerGroup) {
        Date updateTime = new Date(System.currentTimeMillis() - EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS);
        return clientMetaInfoStore.queryClientsUpdateAfterDate(subject, consumerGroup, ClientType.CONSUMER, OnOfflineState.ONLINE, updateTime);
    }

    private String createPartitionName(Partition partition) {
        return createPartitionName(partition.getSubject(), partition.getPartitionId());
    }

    private String createPartitionName(String subject, int physicalPartition) {
        return subject + "#" + physicalPartition;
    }

    private List<Integer> createIntList(int size) {
        ArrayList<Integer> intList = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            intList.add(i);
        }
        return intList;
    }
}
