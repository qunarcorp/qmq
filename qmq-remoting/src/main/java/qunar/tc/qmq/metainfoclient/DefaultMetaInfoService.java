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
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.common.OrderedConstants.ORDERED_CLIENT_HEARTBEAT_INTERVAL_SECS;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class DefaultMetaInfoService implements MetaInfoService, MetaInfoClient.ResponseSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetaInfoService.class);

    private static final long REFRESH_INTERVAL_SECONDS = 60;

    private final ConcurrentHashMap<String, MetaInfoRequestParam> heartbeatRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MetaInfoRequestParam> orderedHeartbeatRequests = new ConcurrentHashMap<>();

    private final ReentrantLock updateLock = new ReentrantLock();

    private long lastUpdateTimestamp = -1;

    private final MetaInfoClient metaInfoClient;

    private ConsumerStateChangedListener consumerStateChangedListener;
    private ConsumerOnlineStateManager consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();

    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService orderedHeartbeatExecutor;

    private String clientId;
    private String metaServer;

    public DefaultMetaInfoService(String metaServer) {
        this.metaServer = metaServer;
        this.metaInfoClient = MetaInfoClientNettyImpl.getClient(new MetaServerLocator(this.metaServer));
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

    private boolean registerHeartbeat(MetaInfoRequestParam param) {
        String key = createKey(param);
        return heartbeatRequests.putIfAbsent(key, param) == null;
    }

    private String createKey(MetaInfoRequestParam param) {
        return createKey(param.getSubject(), param.getGroup());
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
     * @param subject    subject
     * @param group      consumer group
     * @param clientType client type
     * @param appCode    app code
     */
    @Override
    public void registerHeartbeat(String subject, String group, ClientType clientType, String appCode) {
        // 注册心跳
        DefaultMetaInfoService.MetaInfoRequestParam heartbeatParam =
                DefaultMetaInfoService.buildRequestParam(clientType, subject, group, appCode);
        if (this.registerHeartbeat(heartbeatParam)) {
            // 注册一个手动触发一次心跳任务
            triggerConsumerMetaInfoRequest(ClientRequestType.ONLINE);
        }
    }

    private boolean isOrdered(MetaInfoResponse response) {
        if (response instanceof ProducerMetaInfoResponse) {
            return ((ProducerMetaInfoResponse) response).getPartitionMapping() != null;
        } else if (response instanceof ConsumerMetaInfoResponse) {
            return ((ConsumerMetaInfoResponse) response).getConsumerAllocation() != null;
        }
        throw new IllegalStateException(String.format("无法识别的 response %s", response.getClass()));
    }

    private void heartbeat(ConcurrentHashMap<String, MetaInfoRequestParam> requestMap) {
        for (MetaInfoRequestParam param : requestMap.values()) {
            heartbeatRequest(param);
        }
    }

    private void heartbeatRequest(MetaInfoRequestParam param) {
        try {
            Metrics.counter("qmq_pull_metainfo_request_count", SUBJECT_GROUP_ARRAY, new String[]{param.subject, param.group}).inc();
            ClientRequestType requestType = param.hasOnline() ? ClientRequestType.HEARTBEAT : ClientRequestType.ONLINE;
            request(param, requestType);
        } catch (Exception e) {
            LOGGER.debug("request meta info exception. {} {} {}", param.clientType.name(), param.subject, param.group, e);
            Metrics.counter("qmq_pull_metainfo_request_fail", SUBJECT_GROUP_ARRAY, new String[]{param.subject, param.group}).inc();
        }
    }

    public void request(ConcurrentHashMap<String, MetaInfoRequestParam> requestMap, ClientRequestType requestType) {
        for (MetaInfoRequestParam param : requestMap.values()) {
            request(param, requestType);
        }
    }

    public ListenableFuture<MetaInfoResponse> request(MetaInfoRequestParam param, ClientRequestType requestType) {
        return request(
                param.getSubject(),
                param.getGroup(),
                param.getClientType(),
                param.getAppCode(),
                requestType
        );
    }

    @Override
    public ListenableFuture<MetaInfoResponse> request(String subject, String group, ClientType clientType, String appCode, ClientRequestType requestType) {
        MetaInfoRequest request = new MetaInfoRequest();
        request.setSubject(subject);
        request.setClientType(clientType);
        request.setClientId(this.clientId);
        request.setConsumerGroup(group);
        request.setAppCode(appCode);
        boolean online = consumerOnlineStateManager.isOnline(subject, group, this.clientId);
        request.setOnlineState(online ? OnOfflineState.ONLINE : OnOfflineState.OFFLINE);
        request.setRequestType(requestType);

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

    public static MetaInfoRequestParam buildRequestParam(ClientType clientType, String subject, String group, String appCode) {
        return new MetaInfoRequestParam(clientType, subject, group, appCode);
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
        if (isOrdered(response)) {
            // 转移 order 心跳请求到 order map
            MetaInfoRequestParam param = heartbeatRequests.remove(heartbeatKey);
            if (param != null) {
                param.setHasOnline(true);
                orderedHeartbeatRequests.put(heartbeatKey, param);
            }
        } else {
            MetaInfoRequestParam param = heartbeatRequests.get(heartbeatKey);
            param.setHasOnline(true);
        }
    }

    public static final class MetaInfoRequestParam {
        private final ClientType clientType;
        private final String subject;
        private final String group;
        private final String appCode;
        private boolean hasOnline;

        MetaInfoRequestParam(ClientType clientType, String subject, String group, String appCode) {
            this.clientType = clientType;
            this.subject = Strings.nullToEmpty(subject);
            this.group = Strings.nullToEmpty(group);
            this.appCode = appCode;
            this.hasOnline = false;
        }

        public ClientType getClientType() {
            return clientType;
        }

        public String getSubject() {
            return subject;
        }

        public String getGroup() {
            return group;
        }

        public String getAppCode() {
            return appCode;
        }

        public boolean hasOnline() {
            return hasOnline;
        }

        public MetaInfoRequestParam setHasOnline(boolean hasOnline) {
            this.hasOnline = hasOnline;
            return this;
        }

        @Override
        public int hashCode() {
            return clientType.getCode() + 31 * subject.hashCode() + 31 * group.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (!(obj instanceof MetaInfoRequestParam)) return false;
            MetaInfoRequestParam param = (MetaInfoRequestParam) obj;
            return clientType.getCode() == param.getClientType().getCode()
                    && subject.equals(param.subject) && group.equals(param.group);
        }

        @Override
        public String toString() {
            return "MetaInfoParam{" +
                    "clientType=" + clientType.name() +
                    ", subject='" + subject + '\'' +
                    ", group='" + group + '\'' +
                    '}';
        }
    }
}
