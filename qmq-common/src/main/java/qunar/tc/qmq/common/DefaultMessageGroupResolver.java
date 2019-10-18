package qunar.tc.qmq.common;

import com.google.common.base.Preconditions;
import io.netty.util.internal.ThreadLocalRandom;
import java.util.List;
import java.util.Objects;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.meta.PartitionPropsUtils;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.utils.DelayUtil;

/**
 * @author zhenwei.liu
 * @since 2019-09-06
 */
public class DefaultMessageGroupResolver implements MessageGroupResolver {

    private BrokerService brokerService;

    public DefaultMessageGroupResolver(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    public MessageGroup resolveRandomAvailableGroup(String subject, ClientType clientType) {
        // 获取可用的 brokerGroup
        BrokerClusterInfo brokerCluster = brokerService.getProducerBrokerCluster(clientType, subject);
        List<BrokerGroupInfo> brokerGroups = brokerCluster.getGroups();
        int startIndex = ThreadLocalRandom.current().nextInt(0, brokerGroups.size());
        BrokerGroupInfo brokerGroup = getRandomAvailableBrokerGroup(brokerCluster, startIndex);
        if (brokerGroup == null) {
            throw new IllegalStateException("无法找到可用 broker group");
        }

        String brokerGroupName = brokerGroup.getGroupName();

        ProducerAllocation producerAllocation = brokerService.getProducerAllocation(clientType, subject);
        List<PartitionProps> brokerPartitions = PartitionPropsUtils.getPartitionPropsByBrokerGroup(brokerGroupName,
                producerAllocation.getLogical2SubjectLocation().asMapOfRanges().values());
        PartitionProps partitionProps = brokerPartitions.get(ThreadLocalRandom.current().nextInt(0, brokerPartitions.size()));
        String partitionName = partitionProps.getPartitionName();

        return new MessageGroup(clientType, subject, partitionName, brokerGroupName);
    }

    @Override
    public MessageGroup resolveGroup(BaseMessage message) {
        String subject = message.getSubject();
        String orderKey = message.getOrderKey();
        ClientType clientType = DelayUtil.isDelayMessage(message) ? ClientType.DELAY_PRODUCER : ClientType.PRODUCER;
        ProducerAllocation producerAllocation = brokerService.getProducerAllocation(clientType, subject);

        int logicalPartition;
        if (orderKey == null) {
            // 无序消息 orderKey 为 null, 此时从所有 BrokerGroup 中随机选择
            logicalPartition = ThreadLocalRandom.current().nextInt(0, PartitionConstants.DEFAULT_LOGICAL_PARTITION_NUM);
        } else {
            logicalPartition = Math
                    .abs(computeOrderIdentifier(orderKey) % PartitionConstants.DEFAULT_LOGICAL_PARTITION_NUM);
        }

        PartitionProps partitionProps = producerAllocation.getLogical2SubjectLocation().get(logicalPartition);
        Preconditions.checkNotNull(partitionProps,
                "无法找到逻辑分区对应的 broker, subject %s, logicalPartition %s", subject, logicalPartition);
        String brokerGroup = partitionProps.getBrokerGroup();
        String partitionName = partitionProps.getPartitionName();

        if (clientType != ClientType.DELAY_PRODUCER) {
            // DELAY 消息在 delay-server 再来设置这些属性
            message.setProperty(BaseMessage.keys.qmq_subject, subject);
            message.setProperty(BaseMessage.keys.qmq_logicPartition, logicalPartition);
            message.setProperty(BaseMessage.keys.qmq_partitionName, partitionName);
            message.setProperty(BaseMessage.keys.qmq_partitionBroker, brokerGroup);
            message.setProperty(BaseMessage.keys.qmq_partitionVersion, producerAllocation.getVersion());
        }

        return new MessageGroup(clientType, subject, partitionName, brokerGroup);
    }

    @Override
    public MessageGroup resolveAvailableGroup(BaseMessage message) {
        MessageGroup messageGroup = resolveGroup(message);
        String subject = message.getSubject();
        ClientType clientType = DelayUtil.isDelayMessage(message) ? ClientType.DELAY_PRODUCER : ClientType.PRODUCER;
        // best tried 策略当分区不可用时, 会使用另一个可用分区
        BrokerClusterInfo brokerCluster = brokerService.getProducerBrokerCluster(clientType, subject);

        int brokerGroupIdx = 0;
        List<BrokerGroupInfo> brokerGroups = brokerCluster.getGroups();
        String defaultBrokerGroup = messageGroup.getBrokerGroup();
        BrokerGroupInfo brokerGroup = brokerCluster.getGroupByName(defaultBrokerGroup);
        while (!brokerGroup.isAvailable() && brokerGroupIdx < brokerGroups.size()) {
            brokerGroup = brokerGroups.get(brokerGroupIdx);
            brokerGroupIdx++;
        }

        String brokerGroupName = brokerGroup.getGroupName();

        if (Objects.equals(defaultBrokerGroup, brokerGroupName)) {
            return messageGroup;
        }

        // 切换成其他 PartitionName 和 PartitionGroup
        ProducerAllocation producerAllocation = brokerService.getProducerAllocation(clientType, subject);
        List<PartitionProps> props = PartitionPropsUtils
                .getPartitionPropsByBrokerGroup(
                        brokerGroupName,
                        producerAllocation.getLogical2SubjectLocation().asMapOfRanges().values()
                );
        String partitionName = props.get(0).getPartitionName();

        if (clientType != ClientType.DELAY_PRODUCER) {
            // DELAY 消息在 delay-server 再来设置这些属性
            message.setProperty(BaseMessage.keys.qmq_partitionName, partitionName);
            message.setProperty(BaseMessage.keys.qmq_partitionBroker, brokerGroupName);
        }
        return new MessageGroup(clientType, subject, partitionName, brokerGroupName);
    }

    private static int computeOrderIdentifier(String orderKey) {
        int hash = 0;
        if (orderKey.length() > 0) {
            for (int i = 0; i < orderKey.length(); i++) {
                hash = 31 * hash + orderKey.charAt(i);
            }
        }
        return hash;
    }

    private static BrokerGroupInfo getRandomAvailableBrokerGroup(BrokerClusterInfo brokerCluster, int startIndex) {
        List<BrokerGroupInfo> brokerGroups = brokerCluster.getGroups();
        BrokerGroupInfo brokerGroup = null;

        for (int i = startIndex; i < brokerGroups.size() + startIndex; i++) {
            brokerGroup = brokerGroups.get(i % brokerGroups.size());
            if (brokerGroup.isAvailable()) {
                break;
            }
        }

        return brokerGroup;
    }
}
