package qunar.tc.qmq.broker.impl;

import qunar.tc.qmq.broker.BrokerLoadBalance;

/**
 * @author zhenwei.liu
 * @since 2019-09-08
 */
public class BrokerLoadBalanceFactory {

    private static final BrokerLoadBalance instance = new PollBrokerLoadBalance();

    public static BrokerLoadBalance get() {
        return instance;
    }
}
