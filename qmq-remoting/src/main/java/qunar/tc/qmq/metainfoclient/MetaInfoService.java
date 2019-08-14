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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoService implements MetaInfoClient.ResponseSubscriber, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaInfoService.class);

    private static final long REFRESH_INTERVAL_SECONDS = 60;

    private final EventBus eventBus = new EventBus("meta-info");

    private final ConcurrentHashMap<MetaInfoRequestParam, Integer> metaInfoRequests = new ConcurrentHashMap<>();

    private final ReentrantLock updateLock = new ReentrantLock();

    private long lastUpdateTimestamp = -1;

    private final MetaInfoClient client;

    private ConsumerStateChangedListener consumerStateChangedListener;

    private String clientId;
    private String metaServer;

    public MetaInfoService() {
        this.client = MetaInfoClientNettyImpl.getClient();
    }

    public void init() {
        Preconditions.checkNotNull(metaServer, "meta server必须提供");
        this.client.setMetaServerLocator(new MetaServerLocator(metaServer));
        this.client.registerResponseSubscriber(this);
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-meta-refresh"))
                .scheduleAtFixedRate(this, REFRESH_INTERVAL_SECONDS, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public void register(Object subscriber) {
        eventBus.register(subscriber);
    }

    public boolean tryAddRequest(MetaInfoRequestParam param) {
        return metaInfoRequests.put(param, 1) == null;
    }

    @Override
    public void run() {
        for (Map.Entry<MetaInfoRequestParam, Integer> entry : metaInfoRequests.entrySet()) {
            requestWrapper(entry.getKey());
        }
    }

    public void requestWrapper(MetaInfoRequestParam param) {
        try {
            Metrics.counter("qmq_pull_metainfo_request_count", SUBJECT_GROUP_ARRAY, new String[]{param.subject, param.group}).inc();
            request(param);
        } catch (Exception e) {
            LOGGER.debug("request meta info exception. {} {} {}", param.clientType.name(), param.subject, param.group, e);
            Metrics.counter("qmq_pull_metainfo_request_fail", SUBJECT_GROUP_ARRAY, new String[]{param.subject, param.group}).inc();
        }
    }

    private void request(MetaInfoRequestParam param) {
        MetaInfoRequest request = new MetaInfoRequest();
        request.setSubject(param.subject);
        request.setClientType(param.clientType);
        request.setClientId(this.clientId);
        request.setConsumerGroup(param.group);
        request.setAppCode(param.getAppCode());

        if (tryAddRequest(param)) {
            request.setRequestType(ClientRequestType.ONLINE);
        } else {
            request.setRequestType(ClientRequestType.HEARTBEAT);
        }

        LOGGER.debug("meta info request: {}", request);
        client.sendRequest(request);
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        updateConsumerState(response);

        MetaInfo metaInfo = parseResponse(response);
        if (metaInfo != null) {
            LOGGER.debug("meta info: {}", metaInfo);
            eventBus.post(metaInfo);
        } else {
            LOGGER.warn("request meta info fail, will retry in a few seconds.");
        }
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

    private MetaInfo parseResponse(MetaInfoResponse response) {
        if (response == null) return null;

        String subject = response.getSubject();
        if (Strings.isNullOrEmpty(subject)) return null;

        ClientType clientType = parseClientType(response);
        if (clientType == null) return null;

        BrokerCluster cluster = response.getBrokerCluster();
        List<BrokerGroup> groups = cluster == null ? null : cluster.getBrokerGroups();
        if (groups == null || groups.isEmpty()) {
            return new MetaInfo(subject, clientType, new BrokerClusterInfo());
        }

        List<BrokerGroup> validBrokers = new ArrayList<>(groups.size());
        for (BrokerGroup group : groups) {
            if (inValid(group)) continue;

            BrokerState state = group.getBrokerState();
            if (clientType.isConsumer() && state.canRead()) {
                validBrokers.add(group);
            } else if (clientType.isProducer() && state.canWrite()) {
                validBrokers.add(group);
            }
        }
        if (validBrokers.isEmpty()) {
            return new MetaInfo(subject, clientType, new BrokerClusterInfo());
        }
        List<BrokerGroupInfo> groupInfos = new ArrayList<>(validBrokers.size());
        for (int i = 0; i < validBrokers.size(); i++) {
            BrokerGroup bg = validBrokers.get(i);
            groupInfos.add(new BrokerGroupInfo(i, bg.getGroupName(), bg.getMaster(), bg.getSlaves()));
        }
        BrokerClusterInfo clusterInfo = new BrokerClusterInfo(groupInfos);
        return new MetaInfo(subject, clientType, clusterInfo);
    }

    private boolean inValid(BrokerGroup group) {
        return group == null || Strings.isNullOrEmpty(group.getGroupName()) || Strings.isNullOrEmpty(group.getMaster());
    }

    private ClientType parseClientType(MetaInfoResponse response) {
        return ClientType.of(response.getClientTypeCode());
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

    public void setMetaServer(String metaServer) {
        this.metaServer = metaServer;
    }

    public static final class MetaInfoRequestParam {
        private final ClientType clientType;
        private final String subject;
        private final String group;
        private final String appCode;

        MetaInfoRequestParam(ClientType clientType, String subject, String group, String appCode) {
            this.clientType = clientType;
            this.subject = Strings.nullToEmpty(subject);
            this.group = Strings.nullToEmpty(group);
            this.appCode = appCode;
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
