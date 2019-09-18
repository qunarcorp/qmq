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

package qunar.tc.qmq.broker.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.Versionable;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.ClientMetaManager;
import qunar.tc.qmq.common.ClientLifecycleManagerFactory;
import qunar.tc.qmq.common.ExclusiveConsumerLifecycleManager;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.metainfoclient.MetaInfo;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerServiceImpl implements BrokerService, ClientMetaManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServiceImpl.class);

    private Map<String, SettableFuture<ProducerAllocation>> producerAllocationMap = Maps.newConcurrentMap();
    private Map<String, SettableFuture<ConsumerAllocation>> consumerAllocationMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, SettableFuture<BrokerClusterInfo>> clusterMap = new ConcurrentHashMap<>();

    private final MetaInfoService metaInfoService;
    private String appCode;
    private String clientId;

    public BrokerServiceImpl(String appCode, String clientId, MetaInfoService metaInfoService) {
        this.appCode = appCode;
        this.metaInfoService = metaInfoService;
        this.clientId = clientId;
    }

    private void updateBrokerCluster(String key, BrokerClusterInfo clusterInfo) {
        SettableFuture<BrokerClusterInfo> oldFuture = clusterMap.get(key);

        if (!oldFuture.isDone()) {
            oldFuture.set(clusterInfo);
            return;
        }

        BrokerClusterInfo oldClusterInfo;
        try {
            oldClusterInfo = oldFuture.get();
        } catch (Throwable t) {
            // ignore
            throw new RuntimeException(t);
        }

        if (isEquals(oldClusterInfo, clusterInfo)) {
            return;
        }

        List<BrokerGroupInfo> groups = clusterInfo.getGroups();
        List<BrokerGroupInfo> updated = new ArrayList<>(groups.size());
        for (BrokerGroupInfo group : groups) {
            BrokerGroupInfo oldGroup = oldClusterInfo.getGroupByName(group.getGroupName());
            if (oldGroup == null) {
                updated.add(group);
                continue;
            }

            if (oldGroup.getMaster().equals(group.getMaster())) {
                BrokerGroupInfo copy = new BrokerGroupInfo(group.getGroupIndex(),
                        group.getGroupName(), group.getMaster(), group.getSlaves(), oldGroup.getCircuitBreaker());
                updated.add(copy);
                continue;
            }

            updated.add(group);
        }

        BrokerClusterInfo updatedCluster = new BrokerClusterInfo(updated);
        oldFuture.set(updatedCluster);
    }

    private boolean isEquals(BrokerClusterInfo oldClusterInfo, BrokerClusterInfo clusterInfo) {
        List<BrokerGroupInfo> groups = clusterInfo.getGroups();
        if (groups.size() != oldClusterInfo.getGroups().size()) return false;

        for (BrokerGroupInfo group : groups) {
            BrokerGroupInfo oldGroup = oldClusterInfo.getGroupByName(group.getGroupName());
            if (oldGroup == null) return false;

            if (!oldGroup.getMaster().equals(group.getMaster())) return false;
        }
        return true;
    }

    private void logMetaInfo(MetaInfo metaInfo, boolean error) {
        String msg = "meta server return empty broker, will retry in a few seconds. subject={}, client={}";
        if (error) {
            LOGGER.error(msg, metaInfo.getSubject(), metaInfo.getClientType());
        } else {
            LOGGER.info(msg, metaInfo.getSubject(), metaInfo.getClientType());
        }
    }

    private boolean isEmptyCluster(MetaInfo metaInfo) {
        return metaInfo.getClientType() != ClientType.CONSUMER
                && metaInfo.getClusterInfo().getGroups().isEmpty();
    }

    @Override
    public BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject) {
        return getClusterBySubject(clientType, subject, "", false, false);
    }

    @Override
    public BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered) {
        // 这个key上加group不兼容MetaInfoResponse
        String key = buildBrokerClusterKey(clientType, subject);
        Future<BrokerClusterInfo> future = clusterMap.computeIfAbsent(key, k -> {
            MetaInfoRequest request = new MetaInfoRequest(
                    subject,
                    consumerGroup,
                    clientType.getCode(),
                    appCode,
                    clientId,
                    ClientRequestType.ONLINE,
                    isBroadcast,
                    isOrdered);
            ListenableFuture<MetaInfoResponse> responseFuture = metaInfoService.request(request);
            Futures.addCallback(responseFuture, new FutureCallback<MetaInfoResponse>() {
                @Override
                public void onSuccess(MetaInfoResponse response) {
                    // update cache
                    updatePartitionCache(response);

                    updateOrderedClientLifecycle(response);

                    MetaInfo metaInfo = parseResponse(response);
                    if (metaInfo != null) {
                        LOGGER.debug("meta info: {}", metaInfo);
                        String key = buildBrokerClusterKey(metaInfo.getClientType(), metaInfo.getSubject());
                        SettableFuture<BrokerClusterInfo> clusterFuture = clusterMap.get(key);
                        if (isEmptyCluster(metaInfo)) {
                            logMetaInfo(metaInfo, !clusterFuture.isDone());
                        } else {
                            updateBrokerCluster(key, metaInfo.getClusterInfo());
                        }
                    } else {
                        LOGGER.warn("request meta info fail, will retry in a few seconds.");
                    }
                }

                @Override
                public void onFailure(Throwable t) {

                }
            });
            return SettableFuture.create();
        });
        try {
            return future.get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static String buildBrokerClusterKey(ClientType clientType, String subject) {
        return clientType.name() + ":" + subject;
    }

    @Override
    public void refresh(ClientType clientType, String subject) {
        refresh(clientType, subject, "");
    }

    @Override
    public void refresh(ClientType clientType, String subject, String consumerGroup) {
        metaInfoService.request(clientType.getCode(), subject, consumerGroup);
    }

    @Override
    public String getAppCode() {
        return appCode;
    }

    private void updateOrderedClientLifecycle(MetaInfoResponse response) {
        if (!(response instanceof ConsumerMetaInfoResponse)) {
            return;
        }
        String subject = response.getSubject();
        ProducerAllocation producerAllocation = getProducerAllocation(ClientType.of(response.getClientTypeCode()), subject);
        if (producerAllocation != null) {
            ConsumerMetaInfoResponse consumerResponse = (ConsumerMetaInfoResponse) response;
            String consumerGroup = consumerResponse.getConsumerGroup();
            ConsumerAllocation consumerAllocation = consumerResponse.getConsumerAllocation();
            int version = consumerAllocation.getVersion();
            long expired = consumerAllocation.getExpired();
            List<PartitionProps> partitionProps = consumerAllocation.getPartitionProps();
            ExclusiveConsumerLifecycleManager exclusiveConsumerLifecycleManager = ClientLifecycleManagerFactory.get();
            for (PartitionProps partitionProp : partitionProps) {
                exclusiveConsumerLifecycleManager.refreshLifecycle(subject, consumerGroup, partitionProp.getBrokerGroup(), partitionProp.getPartitionName(), version, expired);
            }
        }
    }

    private void updatePartitionCache(MetaInfoResponse response) {
        ClientType clientType = ClientType.of(response.getClientTypeCode());
        String subject = response.getSubject();
        String group = response.getConsumerGroup();
        if (clientType.isProducer()) {
            String key = createProducerPartitionMappingKey(ClientType.of(response.getClientTypeCode()), subject);
            ProducerMetaInfoResponse producerResponse = (ProducerMetaInfoResponse) response;
            updatePartitionCache(key, producerAllocationMap, producerResponse.getProducerAllocation());
        } else if (clientType.isConsumer()) {
            String key = createConsumerAllocationKey(subject, group, clientId);
            ConsumerMetaInfoResponse consumerResponse = (ConsumerMetaInfoResponse) response;
            updatePartitionCache(key, consumerAllocationMap, consumerResponse.getConsumerAllocation());
        }

    }

    private <T extends Versionable> void updatePartitionCache(String key, Map<String, SettableFuture<T>> map, T newVal) {
        if (newVal == null) return;
        synchronized (key.intern()) {
            SettableFuture<T> future = map.computeIfAbsent(key, k -> SettableFuture.create());
            // 旧的存在, 对比版本
            if (!future.isDone()) {
                future.set(newVal);
            } else {
                try {
                    T oldVal = future.get();
                    int oldVersion = oldVal.getVersion();
                    int newVersion = newVal.getVersion();
                    future.set(newVersion > oldVersion ? newVal : oldVal);
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }
        }
    }

    private String createConsumerAllocationKey(String subject, String group, String clientId) {
        return subject + ":" + group + ":" + clientId;
    }

    private String createProducerPartitionMappingKey(ClientType clientType, String subject) {
        return clientType.name() + ":" + subject;
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

        List<BrokerGroupInfo> groupInfos = new ArrayList<>(groups.size());
        for (int i = 0; i < groups.size(); i++) {
            BrokerGroup bg = groups.get(i);
            if (isInvalid(bg)) {
                continue;
            }
            BrokerGroupInfo brokerGroupInfo = new BrokerGroupInfo(i, bg.getGroupName(), bg.getMaster(), bg.getSlaves());
            brokerGroupInfo.setAvailable(isAvailable(bg, clientType));
            groupInfos.add(brokerGroupInfo);
        }
        BrokerClusterInfo clusterInfo = new BrokerClusterInfo(groupInfos);
        return new MetaInfo(subject, clientType, clusterInfo);
    }

    private boolean isAvailable(BrokerGroup brokerGroup, ClientType clientType) {
        BrokerState state = brokerGroup.getBrokerState();
        return (clientType.isConsumer() && state.canRead()) || (clientType.isProducer() && state.canWrite());
    }

    private boolean isInvalid(BrokerGroup group) {
        return group == null || Strings.isNullOrEmpty(group.getGroupName()) || Strings.isNullOrEmpty(group.getMaster());
    }

    private ClientType parseClientType(MetaInfoResponse response) {
        return ClientType.of(response.getClientTypeCode());
    }

    @Override
    public ProducerAllocation getProducerAllocation(ClientType clientType, String subject) {
        String producerKey = createProducerPartitionMappingKey(clientType, subject);
        SettableFuture<ProducerAllocation> future = producerAllocationMap.computeIfAbsent(producerKey, key -> SettableFuture.create());
        try {
            return future.get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public ConsumerAllocation getConsumerAllocation(String subject, String consumerGroup) {
        String key = createConsumerAllocationKey(subject, consumerGroup, clientId);
        SettableFuture<ConsumerAllocation> future = consumerAllocationMap.computeIfAbsent(key, k -> SettableFuture.create());
        try {
            return future.get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
