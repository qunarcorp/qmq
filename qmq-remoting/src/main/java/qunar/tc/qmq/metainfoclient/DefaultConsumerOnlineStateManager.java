package qunar.tc.qmq.metainfoclient;

import com.google.common.collect.Maps;
import java.util.Map;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.broker.impl.SwitchWaiter.Listener;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public class DefaultConsumerOnlineStateManager implements ConsumerOnlineStateManager {

    private static final DefaultConsumerOnlineStateManager instance = new DefaultConsumerOnlineStateManager();

    public static DefaultConsumerOnlineStateManager getInstance() {
        return instance;
    }

    private volatile boolean healthCheckOnline = false;

    private DefaultConsumerOnlineStateManager() {
    }

    private Map<String, SwitchWaiter> stateMap = Maps.newConcurrentMap();

    @Override
    public void onlineHealthCheck() {
        this.healthCheckOnline = true;
        for (SwitchWaiter value : stateMap.values()) {
            value.on(StatusSource.HEALTHCHECKER);
        }
    }

    @Override
    public void offlineHealthCheck() {
        this.healthCheckOnline = false;
        for (SwitchWaiter value : stateMap.values()) {
            value.off(StatusSource.HEALTHCHECKER);
        }
    }

    @Override
    public void online(String subject, String consumerGroup, StatusSource statusSource) {
        SwitchWaiter switchWaiter = getSwitchWaiter(subject, consumerGroup);
        switchWaiter.on(statusSource);
    }

    @Override
    public void offline(String subject, String consumerGroup, StatusSource statusSource) {
        SwitchWaiter switchWaiter = getSwitchWaiter(subject, consumerGroup);
        switchWaiter.off(statusSource);
    }

    @Override
    public boolean isOnline(String subject, String consumerGroup) {
        String key = createKey(subject, consumerGroup);
        SwitchWaiter switchWaiter = stateMap.get(key);
        if (switchWaiter == null) {
            throw new IllegalStateException(
                    String.format("无法找到 switchWaiter subject %s consumerGroup %s", subject, consumerGroup));
        }
        return switchWaiter.isOnline();
    }

    @Override
    public void addOnlineStateListener(String subject, String consumerGroup, Listener listener) {
        String key = createKey(subject, consumerGroup);
        SwitchWaiter switchWaiter = stateMap.get(key);
        if (switchWaiter == null) {
            throw new IllegalStateException(String.format("SwitchWaiter 不存在 %s %s", subject, consumerGroup));
        }
        switchWaiter.addListener(listener);
    }

    @Override
    public SwitchWaiter registerConsumer(String subject, String consumerGroup) {
        String key = createKey(subject, consumerGroup);
        return stateMap.computeIfAbsent(key, k -> new SwitchWaiter(healthCheckOnline));
    }

    @Override
    public SwitchWaiter getSwitchWaiter(String subject, String consumerGroup) {
        String key = createKey(subject, consumerGroup);
        return stateMap.get(key);
    }

    private String createKey(String subject, String consumerGroup) {
        return subject + ":" + consumerGroup;
    }

}
