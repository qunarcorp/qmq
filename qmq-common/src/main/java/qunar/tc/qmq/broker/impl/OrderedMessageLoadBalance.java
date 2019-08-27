package qunar.tc.qmq.broker.impl;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;

import java.util.List;

import static qunar.tc.qmq.base.BaseMessage.keys.qmq_scheduleReceiveTime;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedMessageLoadBalance implements BrokerLoadBalance {

    @Override
    public BrokerGroupInfo loadBalance(BrokerClusterInfo cluster, BrokerGroupInfo lastGroup,
                                       List<BaseMessage> messages) {
        BaseMessage baseMessage = messages.get(0);
        String groupName = baseMessage.getStringProperty(keys.qmq_partitionBroker);
        BrokerGroupInfo brokerGroup = cluster.getGroupByName(groupName);
        Preconditions.checkNotNull(brokerGroup, "could not find broker group for name %s", groupName);
        return brokerGroup;
    }

    private static boolean isDelay(BaseMessage message) {
        return message.getProperty(qmq_scheduleReceiveTime) != null;
    }
}
