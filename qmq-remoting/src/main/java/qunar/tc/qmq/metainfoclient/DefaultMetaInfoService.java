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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
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
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.QuerySubjectRequest;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.common.PartitionConstants.EXCLUSIVE_CLIENT_HEARTBEAT_INTERVAL_MILLS;
import static qunar.tc.qmq.common.PartitionConstants.EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class DefaultMetaInfoService implements MetaInfoService {

    public enum RequestState {
        IDLE, REQUESTING
    }

    public class MetaInfoRequestWrapper implements Stateful<RequestState> {

        FutureCallback<MetaInfoResponse> callback = new FutureCallback<MetaInfoResponse>() {
            @Override
            public void onSuccess(MetaInfoResponse response) {
                updateConsumerState(response);
                String heartbeatKey = createMetaInfoRequestKey(response.getClientTypeCode(), response.getSubject(), response.getConsumerGroup());
                if (isExclusive(response)) {
                    // 转移 order 心跳请求到 order map
                    MetaInfoRequestWrapper request = metaInfoRequests.remove(heartbeatKey);
                    if (request != null) {
                        orderedMetaInfoRequests.put(heartbeatKey, request);
                    }
                }
                MetaInfoRequestWrapper.this.future.set(response);
                hasOnline = true;
                reset();
            }

            @Override
            public void onFailure(Throwable t) {
                MetaInfoRequestWrapper.this.future.setException(t);
                reset();
            }
        };

        private volatile boolean hasOnline = false;
        private MetaInfoRequest request;
        private SettableFuture<MetaInfoResponse> future = SettableFuture.create();
        private AtomicReference<RequestState> state = new AtomicReference<>(RequestState.IDLE);

        public MetaInfoRequestWrapper(MetaInfoRequest request) {
            this.request = request;
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

        public void updateFuture(ListenableFuture<MetaInfoResponse> future) {
            Futures.addCallback(future, callback);
        }

        public ListenableFuture<MetaInfoResponse> getFuture() {
            return future;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetaInfoService.class);

    private static final long REFRESH_INTERVAL_SECONDS = 60;

    // partitionName => subject
    private final LoadingCache<String, String> partitionName2Subject = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(String partitionName) throws Exception {
                    QuerySubjectRequest request = new QuerySubjectRequest(partitionName);
                    String subject = metaInfoClient.querySubject(request).get();
                    if (subject != null) {
                        return subject;
                    } else {
                        throw new IllegalStateException(String.format("无法找到 %s partition subject 映射", partitionName));
                    }
                }
            });

    private final ConcurrentHashMap<String, MetaInfoRequestWrapper> metaInfoRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MetaInfoRequestWrapper> orderedMetaInfoRequests = new ConcurrentHashMap<>();

    private final ReentrantLock updateLock = new ReentrantLock();

    private long lastUpdateTimestamp = -1;

    private final MetaInfoClient metaInfoClient;

    private ConsumerStateChangedListener consumerStateChangedListener;
    private ConsumerOnlineStateManager consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();

    private ScheduledExecutorService metaInfoRequestExecutor;
    private ScheduledExecutorService orderedMetaInfoRequestExecutor;

    private String clientId;

    public DefaultMetaInfoService(String metaServer) {
        this(new MetaServerLocator(metaServer));
    }

    public DefaultMetaInfoService(MetaServerLocator locator) {
        this.metaInfoClient = MetaInfoClientNettyImpl.getClient(locator);
    }

    public void init() {
        this.metaInfoRequestExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-heartbeat-%s"));
        this.orderedMetaInfoRequestExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-heartbeat-order-%s"));

        this.metaInfoRequestExecutor.scheduleAtFixedRate(() -> scheduleRequest(metaInfoRequests), 0, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
        this.orderedMetaInfoRequestExecutor.scheduleAtFixedRate(() -> scheduleRequest(orderedMetaInfoRequests), 0, EXCLUSIVE_CLIENT_HEARTBEAT_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
    }

    public void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber) {
        this.metaInfoClient.registerResponseSubscriber(subscriber);
    }

    private String createMetaInfoRequestKey(int clientType, String subject, String consumerGroup) {
        return clientType + ":" + subject + ":" + consumerGroup;
    }

    private boolean isExclusive(MetaInfoResponse response) {
        if (response instanceof ConsumerMetaInfoResponse) {
            return Objects.equals(((ConsumerMetaInfoResponse) response).getConsumerAllocation().getConsumeStrategy(), ConsumeStrategy.EXCLUSIVE);
        }
        return false;
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
            Metrics.counter("qmq_pull_metainfo_request_count", SUBJECT_GROUP_ARRAY, new String[]{subject, consumerGroup}).inc();
            ClientRequestType requestType = requestWrapper.hasOnline ? ClientRequestType.HEARTBEAT : ClientRequestType.ONLINE;
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
    public ListenableFuture<MetaInfoResponse> request(int clientType, String subject, String consumerGroup) {
        // Meta 的请求不能存在并发请求
        String key = createMetaInfoRequestKey(clientType, subject, consumerGroup);
        MetaInfoRequestWrapper requestWrapper = metaInfoRequests.get(key);
        Preconditions.checkNotNull(requestWrapper, "subject %s consumerGroup %s meta 请求未注册", subject, consumerGroup);
        return request(requestWrapper);
    }

    @Override
    public ListenableFuture<MetaInfoResponse> request(MetaInfoRequest request) {
        // Meta 的请求不能存在并发请求
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        int clientTypeCode = request.getClientTypeCode();
        String key = createMetaInfoRequestKey(clientTypeCode, subject, consumerGroup);
        MetaInfoRequestWrapper requestWrapper = metaInfoRequests.computeIfAbsent(key, k -> new MetaInfoRequestWrapper(request));
        return request(requestWrapper);
    }

    @Override
    public String getSubject(String partitionName) {
        return partitionName2Subject.getUnchecked(partitionName);
    }

    private ListenableFuture<MetaInfoResponse> request(MetaInfoRequestWrapper requestWrapper) {
        // Meta 的请求不能存在并发请求
        MetaInfoRequest request = requestWrapper.getRequest();
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        if (requestWrapper.compareAndSetState(RequestState.IDLE, RequestState.REQUESTING)) {
            boolean online = consumerOnlineStateManager.isOnline(subject, consumerGroup, this.clientId);
            request.setOnlineState(online ? OnOfflineState.ONLINE : OnOfflineState.OFFLINE);

            LOGGER.debug("meta info request: {}", request);
            ListenableFuture<MetaInfoResponse> future = metaInfoClient.sendMetaInfoRequest(request);
            requestWrapper.updateFuture(future);
            return future;
        } else {
            return requestWrapper.getFuture();
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

            if (!Strings.isNullOrEmpty(consumerGroup)) {
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
}
