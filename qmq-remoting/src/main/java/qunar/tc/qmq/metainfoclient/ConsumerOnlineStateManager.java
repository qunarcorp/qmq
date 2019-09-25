package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.broker.impl.SwitchWaiter;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface ConsumerOnlineStateManager {

    boolean isOnline(String subject, String group, String clientId);

    void registerConsumer(String subject, String group, String clientId, boolean healthCheckOnlineState);

    SwitchWaiter getSwitchWaiter(String subject, String group, String clientId);
}
