package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author zhenwei.liu
 * @since 2019-10-09
 */
public class DefaultPartitionReallocator implements PartitionReallocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionReallocator.class);

    private PartitionService partitionService;
    private CachedMetaInfoManager cachedMetaInfoManager;

    public DefaultPartitionReallocator(PartitionService partitionService, CachedMetaInfoManager cachedMetaInfoManager) {
        this.partitionService = partitionService;
        this.cachedMetaInfoManager = cachedMetaInfoManager;
    }

    @Override
    public void reallocate(String subject, String consumerGroup) {
        List<ClientMetaInfo> onlineConsumers = partitionService.getOnlineExclusiveConsumers(subject, consumerGroup);
        reallocate(subject, consumerGroup, onlineConsumers);
    }

    @Override
    public void reallocate(String subject, String consumerGroup, List<ClientMetaInfo> groupOnlineConsumers) {
        Set<String> groupOnlineConsumerIds = groupOnlineConsumers.stream().map(ClientMetaInfo::getClientId).collect(Collectors.toSet());

        PartitionAllocation allocation = cachedMetaInfoManager.getPartitionAllocation(subject, consumerGroup);
        if (allocation != null) {
            PartitionAllocation.AllocationDetail allocationDetail = allocation.getAllocationDetail();
            Set<String> allocationClientIds = allocationDetail.getClientId2PartitionProps().keySet();

            if (!Objects.equals(allocationClientIds, groupOnlineConsumerIds)) {
                // 如果当前在线列表与当前分配情况发生变更, 触发重分配
                reallocate(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds), allocation.getVersion());
            }
        } else {
            // 首次分配
            reallocate(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds), -1);
        }
    }

    private void reallocate(String subject, String consumerGroup, List<String> groupOnlineConsumerIds, int oldVersion) {

        PartitionSet partitionSet = partitionService.getLatestPartitionSet(subject);
        if (partitionSet == null) {
            // 对于默认的无后缀分区, 顺序 Consumer 不使用分配式, 而是返回给所有 Consumer 抢占式进行
            return;
        }

        // 重新分配
        PartitionAllocation newAllocation = partitionService.allocatePartitions(partitionSet, groupOnlineConsumerIds, consumerGroup);

        // 乐观锁更新
        if (partitionService.updatePartitionAllocation(newAllocation, oldVersion)) {
            // TODO(zhenwei.liu) 重分配成功后给 client 发个拉取通知?
            LOGGER.info("分区重分配成功 subject {} group {} oldVersion {} detail {}",
                    subject, consumerGroup, oldVersion,
                    Arrays.toString(newAllocation.getAllocationDetail().getClientId2PartitionProps().entrySet().toArray()));
        } else {
            LOGGER.warn("分区重分配失败 subject {} group {} oldVersion {}",
                    subject, consumerGroup, oldVersion);
        }
    }
}
