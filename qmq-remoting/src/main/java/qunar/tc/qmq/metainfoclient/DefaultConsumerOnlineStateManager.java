package qunar.tc.qmq.metainfoclient;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsumerOnlineStateManager.class);

    private long lastUpdateTimestamp = -1;

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
            throw new IllegalStateException(String.format("无法找到 switchWaiter subject %s consumerGroup %s", subject, consumerGroup));
        }
        return switchWaiter.isOnline();
    }

    @Override
    public SwitchWaiter registerConsumer(String appCode, String subject, String consumerGroup, String clientId, boolean isBroadcast, boolean isOrdered, MetaInfoService metaInfoService, Runnable offlineCallback) {
        String key = createKey(subject, consumerGroup);
        return stateMap.computeIfAbsent(key, k -> {
            SwitchWaiter switchWaiter = new SwitchWaiter(healthCheckOnline);
            switchWaiter.addListener(isOnline -> {
                if (isOnline) {
                    LOGGER.info("Consumer Online, Subject {} ConsumerGroup {}", subject, consumerGroup);
                } else {
                    // 触发 Consumer 下线清理操作
                    offlineCallback.run();
                }

                // 上下线主动触发心跳
                MetaInfoRequest request = new MetaInfoRequest(
                        subject,
                        consumerGroup,
                        ClientType.CONSUMER.getCode(),
                        appCode,
                        clientId,
                        ClientRequestType.SWITCH_STATE,
                        isBroadcast,
                        isOrdered
                );
                metaInfoService.sendRequest(request);
            });
            return switchWaiter;
        });
    }

    @Override
    public SwitchWaiter getSwitchWaiter(String subject, String consumerGroup) {
        String key = createKey(subject, consumerGroup);
        return stateMap.get(key);
    }

    private String createKey(String subject, String consumerGroup) {
        return subject + ":" + consumerGroup;
    }


    private void updateConsumerOPSStatus(MetaInfoResponse response) {
        final String subject = response.getSubject();
        final String consumerGroup = response.getConsumerGroup();
        String key = getMetaKey(response.getClientTypeCode(), subject, consumerGroup);
        synchronized (key.intern()) {
            try {
                if (isStale(response.getTimestamp(), lastUpdateTimestamp)) {
                    LOGGER.debug("skip response {}", response);
                    return;
                }
                lastUpdateTimestamp = response.getTimestamp();

                if (!Strings.isNullOrEmpty(consumerGroup)) {
                    OnOfflineState onOfflineState = response.getOnOfflineState();
                    LOGGER.debug("消费者状态发生变更 {}/{}:{}", subject, consumerGroup, onOfflineState);
                    SwitchWaiter switchWaiter = getSwitchWaiter(subject, consumerGroup);
                    if (onOfflineState == OnOfflineState.ONLINE) {
                        switchWaiter.on(StatusSource.OPS);
                    } else if (onOfflineState == OnOfflineState.OFFLINE) {
                        switchWaiter.off(StatusSource.OPS);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("update meta info exception. response={}", response, e);
            }
        }
    }


    private String getMetaKey(int clientType, String subject, String consumerGroup) {
        return clientType + ":" + subject + ":" + consumerGroup;
    }

    private boolean isStale(long thisTimestamp, long lastUpdateTimestamp) {
        return thisTimestamp < lastUpdateTimestamp;
    }

    @Override
    public void onSuccess(MetaInfoResponse response) {
        updateConsumerOPSStatus(response);
    }
}
