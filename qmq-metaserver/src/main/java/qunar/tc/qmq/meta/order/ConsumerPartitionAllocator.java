package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.meta.model.ClientMetaInfo;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-10-09
 */
public interface ConsumerPartitionAllocator {

    /**
     * 分区重分配
     *
     * @param subject
     * @param consumerGroup
     */
    void reallocate(String subject, String consumerGroup);

    void reallocate(String subject, String consumerGroup, List<ClientMetaInfo> groupOnlineConsumers);
}
