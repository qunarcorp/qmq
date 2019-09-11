package qunar.tc.qmq.producer.sender;

import com.google.common.base.Preconditions;
import io.netty.util.internal.ThreadLocalRandom;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.SubjectLocation;
import qunar.tc.qmq.meta.SubjectLocationUtils;
import qunar.tc.qmq.utils.DelayUtil;

import java.util.List;
import java.util.Objects;

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
    public MessageGroup resolveGroup(Message message) {
        String subject = message.getSubject();
        String orderKey = message.getOrderKey();
        ClientType clientType = DelayUtil.isDelayMessage(message) ? ClientType.DELAY_PRODUCER : ClientType.PRODUCER;
        ProducerAllocation producerAllocation = brokerService.getProducerAllocation(clientType, subject);

        int logicalPartition;
        if (orderKey == null) {
            // 无序消息 orderKey 为 null, 此时从所有 BrokerGroup 中随机选择
            logicalPartition = ThreadLocalRandom.current().nextInt(0, PartitionConstants.DEFAULT_LOGICAL_PARTITION_NUM);
        } else {
            logicalPartition = computeOrderIdentifier(orderKey) % PartitionConstants.DEFAULT_LOGICAL_PARTITION_NUM;
        }

        SubjectLocation subjectLocation = producerAllocation.getLogical2SubjectLocation().get(logicalPartition);
        Preconditions.checkNotNull(subjectLocation,
                "无法找到逻辑分区对应的 broker, subject %s, logicalPartition %s", subject, logicalPartition);
        String brokerGroup = subjectLocation.getBrokerGroup();
        String partitionName = subjectLocation.getPartitionName();

        message.setProperty(BaseMessage.keys.qmq_logicPartition.name(), logicalPartition);
        message.setProperty(BaseMessage.keys.qmq_partitionName.name(), partitionName);
        message.setProperty(BaseMessage.keys.qmq_partitionBroker.name(), brokerGroup);
        message.setProperty(BaseMessage.keys.qmq_partitionVersion.name(), producerAllocation.getVersion());

        return new MessageGroup(clientType, subject, partitionName, brokerGroup);
    }

    @Override
    public MessageGroup resolveAvailableGroup(Message message) {
        MessageGroup messageGroup = resolveGroup(message);
        String subject = message.getSubject();
        ClientType clientType = DelayUtil.isDelayMessage(message) ? ClientType.DELAY_PRODUCER : ClientType.PRODUCER;
        // best tried 策略当分区不可用时, 会使用另一个可用分区
        BrokerClusterInfo brokerCluster = brokerService.getClusterBySubject(clientType, subject);
        List<BrokerGroupInfo> groups = brokerCluster.getGroups();
        int brokerGroupIdx = 0;
        String defaultBrokerGroup = messageGroup.getBrokerGroup();
        BrokerGroupInfo brokerGroup = brokerCluster.getGroupByName(defaultBrokerGroup);
        while (!brokerGroup.isAvailable() && brokerGroupIdx < groups.size()) {
            brokerGroup = groups.get(brokerGroupIdx);
            brokerGroupIdx++;
        }
        String brokerGroupName = brokerGroup.getGroupName();

        if (Objects.equals(defaultBrokerGroup, brokerGroupName)) {
            return messageGroup;
        }

        ProducerAllocation producerAllocation = brokerService.getProducerAllocation(clientType, subject);
        String partitionName = SubjectLocationUtils
                .getSubjectLocationByBrokerGroup(
                        brokerGroupName,
                        producerAllocation.getLogical2SubjectLocation().asMapOfRanges().values()
                )
                .getPartitionName();
        message.setProperty(BaseMessage.keys.qmq_partitionName.name(), partitionName);
        message.setProperty(BaseMessage.keys.qmq_partitionBroker.name(), brokerGroupName);
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
}
