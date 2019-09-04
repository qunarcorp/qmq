package qunar.tc.qmq.broker.impl;

import com.google.common.collect.Maps;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.OrderedMessageManager;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class AdaptiveBrokerLoadBalance implements BrokerLoadBalance {

    private static final String DEFAULT_LOAD_BALANCE = PollBrokerLoadBalance.class.getName();
    private static volatile BrokerLoadBalance instance = null;

    private Map<String, BrokerLoadBalance> loadBalanceMap = Maps.newHashMap();

    public static BrokerLoadBalance getInstance(OrderedMessageManager orderedMessageManager) {
        if (instance == null) {
            synchronized (AdaptiveBrokerLoadBalance.class) {
                if (instance == null) {
                    instance = new AdaptiveBrokerLoadBalance(orderedMessageManager);
                }
            }
        }
        return instance;
    }

    private AdaptiveBrokerLoadBalance(OrderedMessageManager orderedMessageManager) {
        OrderedBrokerLoadBalance orderedBrokerLoadBalance = new OrderedBrokerLoadBalance(orderedMessageManager);
        PollBrokerLoadBalance pollBrokerLoadBalance = new PollBrokerLoadBalance();

        loadBalanceMap.put(orderedBrokerLoadBalance.getClass().getName(), orderedBrokerLoadBalance);
        loadBalanceMap.put(pollBrokerLoadBalance.getClass().getName(), pollBrokerLoadBalance);
    }

    @Override
    public BrokerGroupInfo loadBalance(BrokerClusterInfo cluster, BrokerGroupInfo lastGroup, BaseMessage message) {
        return selectLoadBalance(message).loadBalance(cluster, lastGroup, message);
    }

    private BrokerLoadBalance selectLoadBalance(BaseMessage message) {
        Object loadBalanceType = message.getProperty(keys.qmq_loadBalanceType);
        if (loadBalanceType == null) {
            return loadBalanceMap.get(DEFAULT_LOAD_BALANCE);
        }
        return loadBalanceMap.get(loadBalanceType.toString());
    }
}
