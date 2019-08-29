package qunar.tc.qmq.metainfoclient;

import com.google.common.collect.Maps;

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

    private Map<String, OnlineStateGetter> stateMap = Maps.newConcurrentMap();

    @Override
    public boolean isOnline(String subject, String group, String clientId) {
        String key = createKey(subject, group, clientId);
        OnlineStateGetter onlineStateGetter = stateMap.get(key);
        if (onlineStateGetter == null) {
            throw new IllegalArgumentException(String.format("无法找到 onlineStateGetter subject %s group %s clientId %s", subject, group, clientId));
        }
        return onlineStateGetter.isOnline();
    }

    @Override
    public void registerConsumer(String subject, String group, String clientId, OnlineStateGetter onlineStateGetter) {
        stateMap.put(createKey(subject, group, clientId), onlineStateGetter);
    }

    private String createKey(String subject, String group, String clientId) {
        return subject + ":" + group + ":" + clientId;
    }
}
