package qunar.tc.qmq.common;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.base.OrderStrategyManager;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.OrderStrategy;
import qunar.tc.qmq.broker.impl.OrderedMessageLoadBalance;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.producer.sender.OrderedQueueSender;
import qunar.tc.qmq.utils.DelayUtil;

import java.util.Collection;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public class OrderedMessageUtils {

    public static boolean isOrderedMessage(BaseMessage message) {
        // 延迟消息不走顺序逻辑
        return message.getOrderKey() != null && !DelayUtil.isDelayMessage(message);
    }

    public static void initOrderedMessage(BaseMessage message) {
        if (!isOrderedMessage(message)) {
            return;
        }
        message.setProperty(keys.qmq_queueSenderType, OrderedQueueSender.class.getName());
        message.setProperty(keys.qmq_loadBalanceType, OrderedMessageLoadBalance.class.getName());
    }

    public static void initPartition(BaseMessage message, PartitionMapping partitionMapping, BrokerClusterInfo brokerCluster) {
        if (partitionMapping == null) {
            throw new IllegalStateException(String.format("%s 顺序消息必须先申请分配 partition, 请联系管理员进行申请", message.getSubject()));
        }

        String subject = message.getSubject();
        String orderKey = message.getOrderKey();

        int logicalPartition = computeOrderIdentifier(orderKey) % partitionMapping.getLogicalPartitionNum();
        OrderStrategy orderStrategy = OrderStrategyManager.getOrderStrategy(subject);
        Partition partition = partitionMapping.getLogical2PhysicalPartition().get(logicalPartition);
        Preconditions.checkNotNull(partition,
                "physical partition could not be found, subject %s, logicalPartition %s", subject, logicalPartition);
        String brokerGroupName = partition.getBrokerGroup();

        BrokerGroupInfo realBrokerGroup = orderStrategy.getBrokerGroup(brokerGroupName, brokerCluster);
        String realBrokerGroupName = realBrokerGroup.getGroupName();

        message.setProperty(keys.qmq_logicPartition, logicalPartition);
        message.setProperty(keys.qmq_physicalPartition, getPhysicalPartitionByBrokerGroup(realBrokerGroupName, partitionMapping));
        message.setProperty(keys.qmq_partitionBroker, realBrokerGroupName);
        message.setProperty(keys.qmq_partitionVersion, partitionMapping.getVersion());

    }

    private static int getPhysicalPartitionByBrokerGroup(String groupName, PartitionMapping partitionMapping) {
        Collection<Partition> partitions = partitionMapping.getLogical2PhysicalPartition().asMapOfRanges().values();
        for (Partition partition : partitions) {
            if (Objects.equals(partition.getBrokerGroup(), groupName)) {
                return partition.getPhysicalPartition();
            }
        }
        throw new IllegalArgumentException(String.format("无法找到 brokerGroup %s", groupName));
    }


    public static String getOrderedMessageSubject(String subject, int physicalPartition) {
        return subject + "#" + physicalPartition;
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
