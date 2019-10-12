package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.model.ClientMetaInfo;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class PartitionAllocationTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionAllocationTask.class);

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("partition-allocation-thread-%s").build()
    );

    private PartitionService partitionService;
    private ConsumerPartitionAllocator consumerPartitionAllocator;

    public PartitionAllocationTask(PartitionService partitionService, ConsumerPartitionAllocator consumerPartitionAllocator) {
        this.partitionService = partitionService;
        this.consumerPartitionAllocator = consumerPartitionAllocator;
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                updatePartitionAllocation();
            } catch (Throwable t) {
                LOGGER.error("检查顺序消息分配失败 ", t);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void updatePartitionAllocation() {
        // 当前 client 在线列表
        List<ClientMetaInfo> onlineConsumers = partitionService.getOnlineExclusiveConsumers();

        // 当前分配情况
        Map<String, List<ClientMetaInfo>> onlineConsumerMap = Maps.newHashMap();

        for (ClientMetaInfo consumer : onlineConsumers) {
            String key = consumer.getSubject() + ":" + consumer.getConsumerGroup();
            List<ClientMetaInfo> consumerGroup = onlineConsumerMap.computeIfAbsent(key, k -> Lists.newArrayList());
            consumerGroup.add(consumer);
        }

        for (Map.Entry<String, List<ClientMetaInfo>> entry : onlineConsumerMap.entrySet()) {
            List<ClientMetaInfo> groupOnlineConsumers = entry.getValue();
            String subject = groupOnlineConsumers.get(0).getSubject();
            String consumerGroup = groupOnlineConsumers.get(0).getConsumerGroup();
            consumerPartitionAllocator.reallocate(subject, consumerGroup, groupOnlineConsumers);
        }
    }
}
