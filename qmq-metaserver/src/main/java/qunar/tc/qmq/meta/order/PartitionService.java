package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.meta.model.ClientMetaInfo;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface PartitionService {

    /**
     * 注册新的顺序消息主题
     *
     * @param subject              主题
     * @param physicalPartitionNum 物理分区数量
     */
    boolean registerOrderedMessage(String subject, int physicalPartitionNum);

    ProducerAllocation getDefaultProducerAllocation(String subject, List<BrokerGroup> brokerGroups);

    ConsumerAllocation getDefaultConsumerAllocation(String subject, ConsumeStrategy consumeStrategy, List<BrokerGroup> brokerGroups);

    ConsumerAllocation getConsumerAllocation(String subject, String consumerGroup, ConsumeStrategy consumeStrategy, String clientId);

    /**
     * 获取正在生效的分区分配列表
     *
     * @return 分区分配列表
     */
    List<PartitionAllocation> getActivatedPartitionAllocations();

    PartitionAllocation getActivatedPartitionAllocation(String subject, String group);

    /**
     * 获取最新的分区映射
     *
     * @return 分区映射
     */
    List<ProducerAllocation> getLatestProducerAllocations();

    ProducerAllocation getLatestProducerAllocation(String subject);

    PartitionSet getLatestPartitionSet(String subject);


    /**
     * 为 consumer 分配 partition
     *
     * @param partitionSet       待分配的 partition
     * @param onlineConsumerList online  consumer list
     * @return 分配结果
     */
    PartitionAllocation allocatePartitions(PartitionSet partitionSet, List<String> onlineConsumerList, String consumerGroup);

    /**
     * 更新分区分配信息
     *
     * @param newAllocation 新的分配信息
     * @param baseVersion   分配比对的版本号, 用于做乐观锁
     * @return 更新结果, 若返回 false, 有可能是乐观锁更新失败
     */
    boolean updatePartitionAllocation(PartitionAllocation newAllocation, int baseVersion);

    List<ClientMetaInfo> getOnlineOrderedConsumers();

    List<ClientMetaInfo> getOnlineOrderedConsumers(String subject, String consumerGroup);
}
