package qunar.tc.qmq.producer;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.broker.impl.OrderedMessageLoadBalance;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.producer.sender.OrderedQueueSender;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public class OrderedMessageUtils {

    public static void initOrderedMessage(Message message, int orderIdentifier, PartitionInfo partitionInfo) {
        String subject = message.getSubject();
        int logicalPartition = orderIdentifier % partitionInfo.getLogicalPartitionNum();
        Integer physicalPartition = partitionInfo.getLogicalPartitionMap().get(logicalPartition);
        Preconditions.checkNotNull(physicalPartition,
                "physical partition could not be found, subject %s, logicalPartition %s", subject, logicalPartition);
        BrokerGroup brokerGroup = partitionInfo.getPhysicalPartitionMap().get(physicalPartition);
        Preconditions.checkNotNull(brokerGroup,
                "broker group could not be found, subject %s, logicalPartition %s, physicalPartition %s", subject,
                logicalPartition, physicalPartition);

        BaseMessage baseMessage = (BaseMessage) message;
        baseMessage.setProperty(keys.qmq_logicPartition, logicalPartition);
        baseMessage.setProperty(keys.qmq_physicalPartition, physicalPartition);
        baseMessage.setProperty(keys.qmq_partitionBroker, brokerGroup.getGroupName());
        baseMessage.setProperty(keys.qmq_partitionVersion, partitionInfo.getVersion());
        baseMessage.setProperty(keys.qmq_queueSenderType, OrderedQueueSender.class.getName());
        baseMessage.setProperty(keys.qmq_queueLoadBalanceType, OrderedMessageLoadBalance.class.getName());
    }
}
