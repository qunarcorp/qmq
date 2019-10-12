package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.route.SubjectRouter;

/**
 * @author zhenwei.liu
 * @since 2019-10-09
 */
public class DefaultPartitionReallocator implements ConsumerPartitionAllocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionReallocator.class);

    private PartitionService partitionService;
    private CachedMetaInfoManager cachedMetaInfoManager;
    private SubjectRouter subjectRouter;

    public DefaultPartitionReallocator(PartitionService partitionService, CachedMetaInfoManager cachedMetaInfoManager,
            SubjectRouter subjectRouter) {
        this.partitionService = partitionService;
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.subjectRouter = subjectRouter;
    }

    @Override
    public void reallocate(String subject, String consumerGroup) {
        List<ClientMetaInfo> onlineConsumers = partitionService.getOnlineExclusiveConsumers(subject, consumerGroup);
        reallocate(subject, consumerGroup, onlineConsumers);
    }

    @Override
    public void reallocate(String subject, String consumerGroup, List<ClientMetaInfo> groupOnlineConsumers) {
        if (CollectionUtils.isEmpty(groupOnlineConsumers)) {
            return;
        }
        Set<String> groupOnlineConsumerIds = groupOnlineConsumers.stream().map(ClientMetaInfo::getClientId)
                .collect(Collectors.toSet());

        PartitionAllocation allocation = cachedMetaInfoManager.getPartitionAllocation(subject, consumerGroup);
        if (allocation != null) {
            PartitionAllocation.AllocationDetail allocationDetail = allocation.getAllocationDetail();
            Set<String> allocationClientIds = allocationDetail.getClientId2PartitionProps().keySet();

            PartitionSet latestPartitionSet = cachedMetaInfoManager.getLatestPartitionSet(subject);

            if (!Objects.equals(allocationClientIds, groupOnlineConsumerIds)
                    || latestPartitionSet.getVersion() > allocation.getPartitionSetVersion()) {
                // 如果当前在线列表与当前分配情况发生变更, 或 partitionSet 扩容缩容, 触发重分配
                reallocate(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds), allocation.getVersion());
            }
        } else {
            // 首次分配
            reallocate(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds), -1);
        }
    }

    private void reallocate(String subject, String consumerGroup, List<String> groupOnlineConsumerIds, int oldVersion) {

        PartitionSet partitionSet = partitionService.getLatestPartitionSet(subject);
        PartitionAllocation newAllocation;
        if (partitionSet == null) {
            // 还没发送过消息
            return;
        }

        // 重新分配
        newAllocation = partitionService.allocatePartitions(partitionSet, groupOnlineConsumerIds, consumerGroup);

        // 乐观锁更新
        if (partitionService.updatePartitionAllocation(newAllocation, oldVersion)) {
            // TODO(zhenwei.liu) 重分配成功后给 client 发个拉取通知?
            LOGGER.info("分区重分配成功 subject {} group {} oldVersion {} detail {}",
                    subject, consumerGroup, oldVersion,
                    Arrays.toString(
                            newAllocation.getAllocationDetail().getClientId2PartitionProps().entrySet().toArray()));
        } else {
            LOGGER.warn("分区重分配失败 subject {} group {} oldVersion {}",
                    subject, consumerGroup, oldVersion);
        }

        // 更新本地缓存
        cachedMetaInfoManager.refreshPartitionAllocations();
    }
}
