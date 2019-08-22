package qunar.tc.qmq.producer;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.broker.impl.OrderedMessageLoadBalance;
import qunar.tc.qmq.meta.PartitionInfo;
import qunar.tc.qmq.producer.sender.OrderedQueueSender;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public class OrderedMessageUtils {

    public static boolean isOrderedMessage(BaseMessage message) {
        return message.getOrderKey() != null;
    }

    public static void initOrderedMessage(BaseMessage message, PartitionInfo partitionInfo) {
        if (!isOrderedMessage(message)) {
            return;
        }

        if (partitionInfo == null) {
            throw new IllegalStateException(String.format("%s 顺序消息必须先申请分配 partition, 请联系管理员进行申请", message.getSubject()));
        }

        String subject = message.getSubject();
        int orderIdentifier = message.getOrderKey();
        int logicalPartition = orderIdentifier % partitionInfo.getLogicalPartitionNum();
        Integer physicalPartition = partitionInfo.getLogical2PhysicalPartition().get(logicalPartition);
        Preconditions.checkNotNull(physicalPartition,
                "physical partition could not be found, subject %s, logicalPartition %s", subject, logicalPartition);
        String brokerGroup = partitionInfo.getPhysicalPartition2Broker().get(physicalPartition);
        String delayBrokerGroup = partitionInfo.getPhysicalPartition2DelayBroker().get(physicalPartition);
        Preconditions.checkNotNull(brokerGroup,
                "broker group could not be found, subject %s, logicalPartition %s, physicalPartition %s", subject,
                logicalPartition, physicalPartition);

        message.setProperty(keys.qmq_logicPartition, logicalPartition);
        message.setProperty(keys.qmq_physicalPartition, physicalPartition);
        message.setProperty(keys.qmq_partitionBroker, brokerGroup);
        message.setProperty(keys.qmq_partitionDelayBroker, delayBrokerGroup);
        message.setProperty(keys.qmq_partitionVersion, partitionInfo.getVersion());
        message.setProperty(keys.qmq_queueSenderType, OrderedQueueSender.class.getName());
        message.setProperty(keys.qmq_queueLoadBalanceType, OrderedMessageLoadBalance.class.getName());
    }
}
