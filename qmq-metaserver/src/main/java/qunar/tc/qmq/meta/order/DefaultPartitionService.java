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
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.model.SubjectRoute;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionService.class);

    private PartitionNameResolver partitionNameResolver;
    private Store store;
    private ClientMetaInfoStore clientMetaInfoStore;
    private PartitionStore partitionStore;
    private PartitionSetStore partitionSetStore;
    private PartitionAllocationStore partitionAllocationStore;
    private PartitionAllocator partitionAllocator;
    private TransactionTemplate transactionTemplate;
    private RangeMapper rangeMapper = new AverageRangeMapper();
    private ItemMapper itemMapper = new AverageItemMapper();


    public DefaultPartitionService(
            PartitionNameResolver partitionNameResolver,
            Store store,
            ClientMetaInfoStore clientMetaInfoStore,
            PartitionStore partitionStore,
            PartitionSetStore partitionSetStore,
            PartitionAllocationStore partitionAllocationStore,
            PartitionAllocator partitionAllocator,
            TransactionTemplate transactionTemplate) {
        this.partitionNameResolver = partitionNameResolver;
        this.store = store;
        this.clientMetaInfoStore = clientMetaInfoStore;
        this.partitionStore = partitionStore;
        this.partitionSetStore = partitionSetStore;
        this.partitionAllocationStore = partitionAllocationStore;
        this.partitionAllocator = partitionAllocator;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public boolean updatePartitions(String subject, int physicalPartitionNum, List<String> brokerGroups) {
        PartitionSet oldPartitionSet = partitionSetStore.getLatest(subject);
        return insertNewPartitions(subject, physicalPartitionNum, oldPartitionSet, brokerGroups);
    }

    private boolean insertNewPartitions(String subject, int newPartitionNum, PartitionSet oldPartitionSet, List<String> brokerGroups) {
        int logicalPartitionNum = PartitionConstants.DEFAULT_LOGICAL_PARTITION_NUM;
        int defaultPhysicalPartitionNum = PartitionConstants.DEFAULT_PHYSICAL_PARTITION_NUM;
        newPartitionNum = newPartitionNum <= 0 ? defaultPhysicalPartitionNum : newPartitionNum;

        // 初始化 physical/logical partition 列表
        int startPartitionId = oldPartitionSet == null ? 0 : Collections.max(oldPartitionSet.getPhysicalPartitions()) + 1;
        List<Integer> physicalPartitionList = createIntList(startPartitionId, newPartitionNum);
        List<Integer> logicalPartitionList = createIntList(0, logicalPartitionNum);

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
            // TODO(zhenwei.liu) 分区扩容还需要测试
            try {
                if (oldPartitionSet != null) {
                    // 关闭旧分区
                    Set<Integer> oldPartitions = oldPartitionSet.getPhysicalPartitions();
                    partitionStore.updatePartitionsByIds(subject, oldPartitions, Partition.Status.NRW);
                }
                SubjectRoute oldRoute = store.selectSubjectRoute(subject);
                if (store.updateSubjectRoute(subject, oldRoute.getVersion(), brokerGroups) > 0) {
                    partitionStore.save(partitions);
                    partitionSetStore.save(partitionSet);
                    return true;
                } else {
                    LOGGER.warn("subject {} 创建分区失败, subject router 更新失败", subject);
                    return false;
                }
            } catch (DuplicateKeyException e) {
                // 并发问题, 忽略
                LOGGER.warn("subject {} 创建分区信息重复", subject);
                return false;
            }
        });
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
    public List<PartitionSet> getAllPartitionSets() {
        return partitionSetStore.getAll();
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

    private List<Integer> createIntList(int startInt, int endIntIncluded) {
        ArrayList<Integer> intList = Lists.newArrayList();
        for (int i = startInt; i < endIntIncluded; i++) {
            intList.add(i);
        }
        return intList;
    }
}
