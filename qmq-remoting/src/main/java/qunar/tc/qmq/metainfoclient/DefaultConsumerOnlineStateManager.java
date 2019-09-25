package qunar.tc.qmq.metainfoclient;

import com.google.common.collect.Maps;
import qunar.tc.qmq.broker.impl.SwitchWaiter;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public class DefaultConsumerOnlineStateManager implements ConsumerOnlineStateManager {

    private static final DefaultConsumerOnlineStateManager instance = new DefaultConsumerOnlineStateManager();

    public static DefaultConsumerOnlineStateManager getInstance() {
        return instance;
    }

    private DefaultConsumerOnlineStateManager() {
    }

    private Map<String, SwitchWaiter> stateMap = Maps.newConcurrentMap();

    @Override
    public boolean isOnline(String subject, String group, String clientId) {
        String key = createKey(subject, group, clientId);
        SwitchWaiter switchWaiter = stateMap.get(key);
        if (switchWaiter == null) {
            throw new IllegalStateException(String.format("无法找到 switchWaiter subject %s consumerGroup %s", subject, group));
        }
        return switchWaiter.isOnline();
    }

    @Override
    public void registerConsumer(String subject, String group, String clientId, boolean healthCheckOnlineState) {
        stateMap.computeIfAbsent(createKey(subject, group, clientId), k -> new SwitchWaiter(healthCheckOnlineState));
    }

    @Override
    public SwitchWaiter getSwitchWaiter(String subject, String group, String clientId) {
        String key = createKey(subject, group, clientId);
        return stateMap.get(key);
    }

    private String createKey(String subject, String group, String clientId) {
        return subject + ":" + group + ":" + clientId;
    }
}
