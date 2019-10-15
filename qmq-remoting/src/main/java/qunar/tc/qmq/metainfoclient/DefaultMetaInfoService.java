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

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.batch.Stateful;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.protocol.QuerySubjectRequest;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class DefaultMetaInfoService implements MetaInfoService {

    public enum RequestState {
        IDLE, REQUESTING
    }

    public class MetaInfoRequestWrapper implements Stateful<RequestState> {

        private final long TIMEOUT_MILLS = TimeUnit.SECONDS.toMillis(5);

        private volatile boolean hasOnline = false;
        private long timeoutTimestamp;
        private MetaInfoRequest request;
        private AtomicReference<RequestState> state = new AtomicReference<>(RequestState.IDLE);

        public MetaInfoRequestWrapper(MetaInfoRequest request) {
            this.request = request;
            refreshTimeout();
        }

        @Override
        public RequestState getState() {
            return state.get();
        }

        @Override
        public void setState(RequestState state) {
            this.state.set(state);
        }

        @Override
        public boolean compareAndSetState(RequestState oldState, RequestState newState) {
            return this.state.compareAndSet(oldState, newState);
        }

        @Override
        public void reset() {
            state.set(RequestState.IDLE);
        }

        public MetaInfoRequest getRequest() {
            return request;
        }

        public void refreshTimeout() {
            this.timeoutTimestamp = System.currentTimeMillis() + TIMEOUT_MILLS;
        }

        public long getTimeout() {
            return timeoutTimestamp;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetaInfoService.class);

    // partitionName => subject
    private final LoadingCache<String, SettableFuture<String>> partitionName2Subject = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, SettableFuture<String>>() {
                @Override
                public SettableFuture<String> load(String partitionName) throws Exception {
                    return SettableFuture.create();
                }
            });

    private final ConcurrentHashMap<String, MetaInfoRequestWrapper> metaInfoRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MetaInfoRequestWrapper> exclusiveConsumerMetaInfoRequests = new ConcurrentHashMap<>();


    private final MetaInfoClient metaInfoClient;

    private ConsumerOnlineStateManager consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();

    private ScheduledExecutorService metaInfoRequestExecutor;
    private ScheduledExecutorService exclusiveConsumerMetaInfoRequestExecutor;

    private String clientId;

    public DefaultMetaInfoService(String metaServer) {
        this(new MetaServerLocator(metaServer));
    }

    public DefaultMetaInfoService(MetaServerLocator locator) {
        this.metaInfoClient = MetaServerNettyClient.getClient(locator);
        this.metaInfoClient.registerResponseSubscriber(response -> {
            // 如果是异常退出, 只能等待 timeout 了
            int clientTypeCode = response.getClientTypeCode();
            String subject = response.getSubject();
            String consumerGroup = response.getConsumerGroup();
            String requestKey = createMetaInfoRequestKey(clientTypeCode, subject, consumerGroup);
            MetaInfoRequestWrapper rw = metaInfoRequests.get(requestKey);
            if (rw == null) {
                return;
            }
            rw.hasOnline = true;
            rw.reset();

            // 更新 metaInfoRequest 的 ConsumeStrategy
            if (clientTypeCode == ClientType.CONSUMER.getCode()) {
                MetaInfoRequest request = rw.getRequest();
                ConsumerMetaInfoResponse cresponse = (ConsumerMetaInfoResponse) response;
                ConsumeStrategy consumeStrategy = cresponse.getConsumerAllocation().getConsumeStrategy();
                request.setConsumeStrategy(consumeStrategy);
                if (consumeStrategy == ConsumeStrategy.EXCLUSIVE) {
                    exclusiveConsumerMetaInfoRequests.putIfAbsent(requestKey, rw);
                }
            }
        });
    }

    public void init() {
        this.metaInfoRequestExecutor = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-heartbeat-%s"));
        this.exclusiveConsumerMetaInfoRequestExecutor = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-exclusive-consumer-heartbeat-%s"));
        this.metaInfoRequestExecutor
                .scheduleAtFixedRate(() -> scheduleRequest(metaInfoRequests), 0,
                        PartitionConstants.CLIENT_HEARTBEAT_INTERVAL_MILLS,
                        TimeUnit.MILLISECONDS);
        this.exclusiveConsumerMetaInfoRequestExecutor
                .scheduleAtFixedRate(() -> scheduleRequest(exclusiveConsumerMetaInfoRequests), 0,
                        PartitionConstants.EXCLUSIVE_CLIENT_HEARTBEAT_INTERVAL_MILLS,
                        TimeUnit.MILLISECONDS);
    }

    public void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber) {
        this.metaInfoClient.registerResponseSubscriber(subscriber);
    }

    private String createMetaInfoRequestKey(int clientType, String subject, String consumerGroup) {
        return clientType + ":" + subject + ":" + consumerGroup;
    }

    private void scheduleRequest(ConcurrentHashMap<String, MetaInfoRequestWrapper> requestMap) {
        for (MetaInfoRequestWrapper requestWrapper : requestMap.values()) {
            scheduleRequest(requestWrapper);
        }
    }

    private void scheduleRequest(MetaInfoRequestWrapper requestWrapper) {
        MetaInfoRequest request = requestWrapper.getRequest();
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        try {
            Metrics.counter("qmq_pull_metainfo_request_count", SUBJECT_GROUP_ARRAY,
                    new String[]{subject, consumerGroup}).inc();
            ClientRequestType requestType =
                    requestWrapper.hasOnline ? ClientRequestType.HEARTBEAT : ClientRequestType.ONLINE;
            request.setRequestType(requestType.getCode());
            triggerHeartbeat(request.getClientTypeCode(), request.getSubject(), request.getConsumerGroup());
        } catch (Exception e) {
            LOGGER.debug("request meta info exception. {} {} {}", ClientType.of(request.getClientTypeCode()), subject,
                    consumerGroup, e);
            Metrics.counter("qmq_pull_metainfo_request_fail", SUBJECT_GROUP_ARRAY, new String[]{subject, consumerGroup})
                    .inc();
        }
    }

    @Override
    public void triggerHeartbeat(int clientType, String subject, String consumerGroup) {
        String key = createMetaInfoRequestKey(clientType, subject, consumerGroup);
        MetaInfoRequestWrapper requestWrapper = metaInfoRequests.get(key);
        Preconditions.checkNotNull(requestWrapper, "subject %s consumerGroup %s meta 请求未注册", subject, consumerGroup);
        request(requestWrapper);
    }

    @Override
    public void registerHeartbeat(String appCode, int clientTypeCode, String subject, String consumerGroup,
            boolean isBroadcast, boolean isOrdered) {
        String key = createMetaInfoRequestKey(clientTypeCode, subject, consumerGroup);
        MetaInfoRequestWrapper requestWrapper = metaInfoRequests.computeIfAbsent(key, k -> {
            MetaInfoRequest request = new MetaInfoRequest(
                    subject,
                    consumerGroup,
                    clientTypeCode,
                    appCode,
                    clientId,
                    ClientRequestType.ONLINE,
                    isBroadcast,
                    isOrdered
            );
            return new MetaInfoRequestWrapper(request);
        });
        request(requestWrapper);
    }

    private void request(MetaInfoRequestWrapper requestWrapper) {
        // Meta 的请求不能存在并发请求
        MetaInfoRequest request = requestWrapper.getRequest();
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        boolean timeout = System.currentTimeMillis() > requestWrapper.getTimeout();
        if (timeout) {
            // 这里可能会出现一些并发请求, 但问题不大
            requestWrapper.reset();
        }
        if (requestWrapper.compareAndSetState(RequestState.IDLE, RequestState.REQUESTING)) {
            requestWrapper.refreshTimeout();
            if (request.getClientTypeCode() == ClientType.CONSUMER.getCode()) {
                boolean online = consumerOnlineStateManager.isOnline(subject, consumerGroup);
                request.setOnlineState(online ? OnOfflineState.ONLINE : OnOfflineState.OFFLINE);
            } else {
                request.setOnlineState(OnOfflineState.ONLINE);
            }

            LOGGER.debug("meta info request: {}", request);
            metaInfoClient.sendMetaInfoRequest(request);
        }
    }

    @Override
    public ChannelFuture sendRequest(MetaInfoRequest request) {
        return metaInfoClient.sendMetaInfoRequest(request);
    }

    @Override
    public String getSubject(String partitionName) {
        SettableFuture<String> future = partitionName2Subject.getUnchecked(partitionName);
        if (!future.isDone()) {
            QuerySubjectRequest request = new QuerySubjectRequest(partitionName);
            metaInfoClient.querySubject(request, response -> future.set(response.getSubject()));
        }
        try {
            return future.get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
