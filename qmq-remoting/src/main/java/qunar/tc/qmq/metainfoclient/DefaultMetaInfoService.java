/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.metainfoclient;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.common.PartitionConstants.ORDERED_CLIENT_HEARTBEAT_INTERVAL_SECS;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class DefaultMetaInfoService implements MetaInfoService, MetaInfoClient.ResponseSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetaInfoService.class);

    private static final long REFRESH_INTERVAL_SECONDS = 60;

    private final ConcurrentHashMap<String, MetaInfoRequest> heartbeatRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MetaInfoRequest> orderedHeartbeatRequests = new ConcurrentHashMap<>();

    private final ReentrantLock updateLock = new ReentrantLock();

    private long lastUpdateTimestamp = -1;

    private final MetaInfoClient metaInfoClient;

    private ConsumerStateChangedListener consumerStateChangedListener;
    private ConsumerOnlineStateManager consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();

    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService orderedHeartbeatExecutor;

    private String clientId;

    public DefaultMetaInfoService(String metaServer) {
        this.metaInfoClient = MetaInfoClientNettyImpl.getClient(new MetaServerLocator(metaServer));
        this.metaInfoClient.registerResponseSubscriber(this);
    }

    public void init() {
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-heartbeat-%s"));
        this.heartbeatExecutor.scheduleAtFixedRate(() -> heartbeat(heartbeatRequests), REFRESH_INTERVAL_SECONDS, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
        this.orderedHeartbeatExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-heartbeat-order-%s"));
        this.orderedHeartbeatExecutor.scheduleAtFixedRate(() -> heartbeat(orderedHeartbeatRequests), ORDERED_CLIENT_HEARTBEAT_INTERVAL_SECS, ORDERED_CLIENT_HEARTBEAT_INTERVAL_SECS, TimeUnit.SECONDS);
    }

    public void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber) {
        this.metaInfoClient.registerResponseSubscriber(subscriber);
    }

    private boolean registerHeartbeat(MetaInfoRequest request) {
        String key = createKey(request);
        return heartbeatRequests.putIfAbsent(key, request) == null;
    }

    private String createKey(MetaInfoRequest request) {
        return createKey(request.getSubject(), request.getConsumerGroup());
    }

    private String createKey(String subject, String group) {
        return subject + ":" + group;
    }

    @Override
    public void triggerConsumerMetaInfoRequest(ClientRequestType requestType) {
        this.orderedHeartbeatExecutor.execute(() -> request(this.orderedHeartbeatRequests, requestType));
        this.heartbeatExecutor.execute(() -> request(this.heartbeatRequests, requestType));
    }

    /**
     * 注册心跳
     *
     * @param subject       subject
     * @param consumerGroup consumer group
     * @param clientType    client type
     * @param appCode       app code
     */
    @Override
    public void registerHeartbeat(String subject, String consumerGroup, ClientType clientType, String appCode) {
        this.registerHeartbeat(subject, consumerGroup, clientType, appCode, null);
    }

    @Override
    public void registerHeartbeat(String subject, String consumerGroup, ClientType clientType, String appCode, ConsumeMode consumeMode) {
        // 注册心跳
        MetaInfoRequest request = new MetaInfoRequest(
                subject,
                consumerGroup,
                clientType.getCode(),
                appCode,
                clientId,
                ClientRequestType.ONLINE,
                consumeMode
        );
        if (this.registerHeartbeat(request)) {
            // 注册一个手动触发一次心跳任务
            triggerConsumerMetaInfoRequest(ClientRequestType.ONLINE);
        }
    }

    private boolean isExclusive(MetaInfoResponse response) {
        if (response instanceof ConsumerMetaInfoResponse) {
            return Objects.equals(((ConsumerMetaInfoResponse) response).getConsumerAllocation().getConsumeMode(), ConsumeMode.EXCLUSIVE);
        }
        return false;
    }

    private void heartbeat(ConcurrentHashMap<String, MetaInfoRequest> requestMap) {
        for (MetaInfoRequest request : requestMap.values()) {
            heartbeatRequest(request);
        }
    }

    private void heartbeatRequest(MetaInfoRequest request) {
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        try {
            Metrics.counter("qmq_pull_metainfo_request_count", SUBJECT_GROUP_ARRAY, new String[]{subject, consumerGroup}).inc();
            ClientRequestType requestType = request.isInited() ? ClientRequestType.HEARTBEAT : ClientRequestType.ONLINE;
            request.setRequestType(requestType.getCode());
            request(request);
        } catch (Exception e) {
            LOGGER.debug("request meta info exception. {} {} {}", ClientType.of(request.getClientTypeCode()), subject, consumerGroup, e);
            Metrics.counter("qmq_pull_metainfo_request_fail", SUBJECT_GROUP_ARRAY, new String[]{subject, consumerGroup}).inc();
        }
    }

    public void request(ConcurrentHashMap<String, MetaInfoRequest> requestMap, ClientRequestType requestType) {
        for (MetaInfoRequest request : requestMap.values()) {
            request.setRequestType(requestType.getCode());
            request(request);
        }
    }

    @Override
    public ListenableFuture<MetaInfoResponse> request(MetaInfoRequest request) {
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        boolean online = consumerOnlineStateManager.isOnline(subject, consumerGroup, this.clientId);
        request.setOnlineState(online ? OnOfflineState.ONLINE : OnOfflineState.OFFLINE);

        LOGGER.debug("meta info request: {}", request);
        return metaInfoClient.sendMetaInfoRequest(request);
    }

    private void updateConsumerState(MetaInfoResponse response) {
        updateLock.lock();
        try {
            if (isStale(response.getTimestamp(), lastUpdateTimestamp)) {
                LOGGER.debug("skip response {}", response);
                return;
            }
            lastUpdateTimestamp = response.getTimestamp();

            final String subject = response.getSubject();
            final String consumerGroup = response.getConsumerGroup();

            if (RetrySubjectUtils.isRealSubject(subject) && !Strings.isNullOrEmpty(consumerGroup)) {
                boolean online = response.getOnOfflineState() == OnOfflineState.ONLINE;
                LOGGER.debug("消费者状态发生变更 {}/{}:{}", subject, consumerGroup, online);
                triggerConsumerStateChanged(subject, consumerGroup, online);
            }

        } catch (Exception e) {
            LOGGER.error("update meta info exception. response={}", response, e);
        } finally {
            updateLock.unlock();
        }
    }

    private boolean isStale(long thisTimestamp, long lastUpdateTimestamp) {
        return thisTimestamp < lastUpdateTimestamp;
    }

    public void setConsumerStateChangedListener(ConsumerStateChangedListener listener) {
        this.consumerStateChangedListener = listener;
    }

    private void triggerConsumerStateChanged(String subject, String consumerGroup, boolean online) {
        if (this.consumerStateChangedListener == null) return;

        if (online) {
            this.consumerStateChangedListener.online(subject, consumerGroup);
        } else {
            this.consumerStateChangedListener.offline(subject, consumerGroup);
        }
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        updateConsumerState(response);
        String heartbeatKey = createKey(response.getSubject(), response.getConsumerGroup());
        if (isExclusive(response)) {
            // 转移 order 心跳请求到 order map
            MetaInfoRequest request = heartbeatRequests.remove(heartbeatKey);
            if (request != null) {
                request.setInited(true);
                orderedHeartbeatRequests.put(heartbeatKey, request);
            }
        } else {
            MetaInfoRequest request = heartbeatRequests.get(heartbeatKey);
            if (request != null) {
                request.setInited(true);
            }
        }
    }
}
