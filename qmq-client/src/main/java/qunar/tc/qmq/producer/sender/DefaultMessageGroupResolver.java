package qunar.tc.qmq.producer.sender;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.util.internal.ThreadLocalRandom;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.OrderStrategyManager;
import qunar.tc.qmq.broker.*;
import qunar.tc.qmq.common.OrderedConstants;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.utils.DelayUtil;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static qunar.tc.qmq.common.OrderedConstants.EMPTY_PARTITION;

/**
 * @author zhenwei.liu
 * @since 2019-09-06
 */
public class DefaultMessageGroupResolver implements MessageGroupResolver {

    private MessageSendContextManager messageSendContextManager;
    private BrokerService brokerService;
    private BrokerLoadBalance brokerLoadBalance;

    public DefaultMessageGroupResolver(MessageSendContextManager messageSendContextManager, BrokerService brokerService, BrokerLoadBalance brokerLoadBalance) {
        this.messageSendContextManager = messageSendContextManager;
        this.brokerService = brokerService;
        this.brokerLoadBalance = brokerLoadBalance;
    }

    @Override
    public MessageGroup resolveGroup(Message message) {
        String subject = message.getSubject();
        String orderKey = message.getOrderKey();
        ClientType clientType = DelayUtil.isDelayMessage(message) ? ClientType.DELAY_PRODUCER : ClientType.PRODUCER;
        PartitionMapping partitionMapping = brokerService.getPartitionMapping(clientType, subject);
        BrokerClusterInfo brokerCluster = brokerService.getClusterBySubject(clientType, subject);

        if (DelayUtil.isDelayMessage(message)) {
            // delay 消息直接选择 broker
            return resolveNonOrderedMessageGroup(clientType, subject, brokerCluster);
        } else {
            int logicalPartition;
            if (orderKey == null) {
                // 无序消息 orderKey 为 null, 此时从所有 BrokerGroup 中随机选择
                logicalPartition = ThreadLocalRandom.current().nextInt(0, OrderedConstants.DEFAULT_LOGICAL_PARTITION_NUM);
            } else {
                logicalPartition = computeOrderIdentifier(orderKey) % partitionMapping.getLogicalPartitionNum();
            }

            Partition partition = partitionMapping.getLogical2PhysicalPartition().get(logicalPartition);
            Preconditions.checkNotNull(partition,
                    "无法找到 physical partition, subject %s, logicalPartition %s", subject, logicalPartition);
            String brokerGroup = partition.getBrokerGroup();
            int physicalPartition = partition.getPhysicalPartition();

            OrderStrategy orderStrategy = OrderStrategyManager.getOrderStrategy(subject);
            if (Objects.equals(orderStrategy, OrderStrategy.BEST_TRIED)) {
                // 非严格有序消息需要选择一个可用的 broker
                BrokerGroupInfo availableBroker = getAvailableBroker(brokerCluster, brokerGroup);
                if (availableBroker == null) {
                    throw new IllegalStateException(String.format("没有可用的 broker subject %s clientType %s", subject, clientType));
                } else {
                    brokerGroup = availableBroker.getGroupName();
                    physicalPartition = getPhysicalPartitionByBrokerGroup(partitionMapping, brokerGroup);
                }
            }

            message.setProperty(BaseMessage.keys.qmq_orderStrategy.name(), orderStrategy.name());
            message.setProperty(BaseMessage.keys.qmq_logicPartition.name(), logicalPartition);
            message.setProperty(BaseMessage.keys.qmq_physicalPartition.name(), physicalPartition);
            message.setProperty(BaseMessage.keys.qmq_partitionBroker.name(), brokerGroup);
            message.setProperty(BaseMessage.keys.qmq_partitionVersion.name(), partitionMapping.getVersion());

            return new MessageGroup(clientType, subject, physicalPartition, brokerGroup);
        }
    }

    private int getPhysicalPartitionByBrokerGroup(PartitionMapping partitionMapping, String brokerGroupName) {
        Collection<Partition> partitions = partitionMapping.getLogical2PhysicalPartition().asMapOfRanges().values();
        for (Partition partition : partitions) {
            if (Objects.equals(brokerGroupName, partition.getBrokerGroup())) {
                return partition.getPhysicalPartition();
            }
        }
        throw new IllegalArgumentException(String.format("无法找到 broker %s 对应的 partition", brokerGroupName));
    }

    private BrokerGroupInfo getAvailableBroker(BrokerClusterInfo brokerCluster, String currentBrokerGroupName) {
        BrokerGroupInfo currentBrokerGroup = brokerCluster.getGroupByName(currentBrokerGroupName);
        if (currentBrokerGroup.isAvailable()) {
            return currentBrokerGroup;
        } else {
            List<BrokerGroupInfo> groups = brokerCluster.getGroups();
            for (BrokerGroupInfo group : groups) {
                if (group.isAvailable()) {
                    return group;
                }
            }
        }
        return null;
    }

    private MessageGroup resolveNonOrderedMessageGroup(ClientType clientType, String subject, BrokerClusterInfo brokerCluster) {
        int partition = EMPTY_PARTITION;
        MessageSendContextManager.ContextKey key = new MessageSendContextManager.ContextKey(clientType, subject, partition);
        MessageSendContextManager.MessageSendContext context = messageSendContextManager.getContext(key);
        String lastSentBrokerName = context.getLastSentBroker();
        BrokerGroupInfo lastSentBrokerGroupInfo = Strings.isNullOrEmpty(lastSentBrokerName) ? null : brokerCluster.getGroupByName(lastSentBrokerName);
        BrokerGroupInfo brokerGroup = brokerLoadBalance.loadBalance(brokerCluster.getGroups(), lastSentBrokerGroupInfo);
        return new MessageGroup(clientType, subject, partition, brokerGroup.getGroupName());
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
