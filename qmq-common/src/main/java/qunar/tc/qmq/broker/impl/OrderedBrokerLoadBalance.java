package qunar.tc.qmq.broker.impl;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.base.OrderStrategyManager;
import qunar.tc.qmq.broker.*;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionMapping;

import java.util.Collection;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedBrokerLoadBalance implements BrokerLoadBalance {

    private OrderedMessageManager orderedMessageManager;

    public OrderedBrokerLoadBalance(OrderedMessageManager orderedMessageManager) {
        this.orderedMessageManager = orderedMessageManager;
    }

    @Override
    public BrokerGroupInfo loadBalance(BrokerClusterInfo brokerCluster, BrokerGroupInfo lastGroup, BaseMessage message) {
        String subject = message.getSubject();
        String orderKey = message.getOrderKey();
        PartitionMapping partitionMapping = orderedMessageManager.getPartitionMapping(subject);
        if (partitionMapping == null) {
            throw new IllegalStateException(String.format("%s 顺序消息必须先申请分配 partition, 请联系管理员进行申请", message.getSubject()));
        }
        int logicalPartition = computeOrderIdentifier(orderKey) % partitionMapping.getLogicalPartitionNum();
        OrderStrategy orderStrategy = OrderStrategyManager.getOrderStrategy(subject);
        Partition partition = partitionMapping.getLogical2PhysicalPartition().get(logicalPartition);
        Preconditions.checkNotNull(partition,
                "无法找到 physical partition, subject %s, logicalPartition %s", subject, logicalPartition);
        String brokerGroupName = partition.getBrokerGroup();

        BrokerGroupInfo realBrokerGroup = orderStrategy.getBrokerGroup(brokerGroupName, brokerCluster);
        String realBrokerGroupName = realBrokerGroup.getGroupName();

        message.setProperty(keys.qmq_logicPartition, logicalPartition);
        message.setProperty(keys.qmq_physicalPartition, getPhysicalPartitionByBrokerGroup(realBrokerGroupName, partitionMapping));
        message.setProperty(keys.qmq_partitionBroker, realBrokerGroupName);
        message.setProperty(keys.qmq_partitionVersion, partitionMapping.getVersion());

        return realBrokerGroup;
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

    private static int getPhysicalPartitionByBrokerGroup(String groupName, PartitionMapping partitionMapping) {
        Collection<Partition> partitions = partitionMapping.getLogical2PhysicalPartition().asMapOfRanges().values();
        for (Partition partition : partitions) {
            if (Objects.equals(partition.getBrokerGroup(), groupName)) {
                return partition.getPhysicalPartition();
            }
        }
        throw new IllegalArgumentException(String.format("无法找到 brokerGroup %s", groupName));
    }
}
