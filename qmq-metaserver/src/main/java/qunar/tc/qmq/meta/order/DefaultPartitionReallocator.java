package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;

/**
 * @author zhenwei.liu
 * @since 2019-10-09
 */
public class DefaultPartitionReallocator implements ConsumerPartitionAllocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionReallocator.class);

    private TransactionTemplate transactionTemplate = JdbcTemplateHolder.getTransactionTemplate();
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
        if (CollectionUtils.isEmpty(groupOnlineConsumers)) {
            return;
        }
        Set<String> groupOnlineConsumerIds = groupOnlineConsumers.stream().map(ClientMetaInfo::getClientId)
                .collect(Collectors.toSet());

        PartitionAllocation oldPartitionAllocation = cachedMetaInfoManager
                .getPartitionAllocation(subject, consumerGroup);

        if (oldPartitionAllocation != null) {
            PartitionSet latestPartitionSet = cachedMetaInfoManager.getLatestPartitionSet(subject);
            if (needReallocation(latestPartitionSet, groupOnlineConsumers, oldPartitionAllocation)) {
                // 如果当前在线列表与当前分配情况发生变更, 或 partitionSet 扩容缩容, 触发重分配
                reallocate(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds),
                        oldPartitionAllocation.getVersion());
            }
        } else {
            // 首次分配
            reallocate(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds), -1);
        }
    }

    private boolean needReallocation(PartitionSet latestPartitionSet, List<ClientMetaInfo> groupOnlineConsumers,
            PartitionAllocation oldPartitionAllocation) {

        if (latestPartitionSet.getVersion() > oldPartitionAllocation.getPartitionSetVersion()) {
            // 分区扩容, 缩容
            return true;
        }

        Set<String> oldConsumerIds = oldPartitionAllocation.getAllocationDetail().getClientId2PartitionProps().keySet();
        Set<String> newConsumerIds = groupOnlineConsumers.stream().map(ClientMetaInfo::getClientId)
                .collect(Collectors.toSet());
        return !Objects.equals(oldConsumerIds, newConsumerIds);
    }

    private void reallocate(String subject, String consumerGroup, List<String> groupOnlineConsumerIds, int oldVersion) {
        transactionTemplate.execute((TransactionCallback<Void>) transactionStatus -> {

            PartitionSet partitionSet = partitionService.getLatestPartitionSet(subject);
            if (partitionSet == null) {
                // 还没发送过消息
                return null;
            }

            // 重新分配
            PartitionAllocation newAllocation = partitionService
                    .allocatePartitions(partitionSet, groupOnlineConsumerIds, consumerGroup);

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
            return null;
        });
    }
}
