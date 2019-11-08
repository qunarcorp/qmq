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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.Versionable;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.ClientMetaManager;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.metainfoclient.MetaInfo;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.protocol.consumer.LockOperationRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerServiceImpl implements BrokerService, ClientMetaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServiceImpl.class);

    private Map<String, SettableFuture<AtomicReference<ProducerAllocation>>> producerAllocationMap = Maps
            .newConcurrentMap();
    private final ConcurrentMap<String, SettableFuture<AtomicReference<BrokerClusterInfo>>> clusterMap = new ConcurrentHashMap<>();

    private final MetaInfoService metaInfoService;
    private final NettyClient nettyClient = NettyClient.getClient();
    private String appCode;
    private String clientId;

    public BrokerServiceImpl(String appCode, String clientId, MetaInfoService metaInfoService) {
        this.appCode = appCode;
        this.metaInfoService = metaInfoService;
        this.clientId = clientId;
        this.metaInfoService.registerResponseSubscriber(response -> {
            // update cache
            updatePartitionCache(response);

            MetaInfo metaInfo = parseResponse(response);
            if (metaInfo != null) {
                LOGGER.debug("meta info: {}", metaInfo);
                String key = buildBrokerClusterKey(metaInfo.getClientType(), metaInfo.getSubject());
                if (isEmptyCluster(metaInfo)) {
                    logMetaInfo(metaInfo);
                } else {
                    updateBrokerCluster(key, metaInfo.getClusterInfo());
                }
            } else {
                LOGGER.warn("request meta info fail, will retry in a few seconds.");
            }
        });
    }

    private void updateBrokerCluster(String key, BrokerClusterInfo newBrokerCluster) {
        SettableFuture<AtomicReference<BrokerClusterInfo>> oldFuture = clusterMap
                .computeIfAbsent(key, k -> SettableFuture.create());

        if (!oldFuture.isDone()) {
            oldFuture.set(new AtomicReference<>(newBrokerCluster));
            return;
        }

        AtomicReference<BrokerClusterInfo> oldRef;
        try {
            oldRef = oldFuture.get();
        } catch (Throwable t) {
            // ignore
            throw new RuntimeException(t);
        }

        BrokerClusterInfo oldBrokerCluster = oldRef.get();
        if (isBrokerClusterEquals(oldBrokerCluster, newBrokerCluster)) {
            return;
        }

        List<BrokerGroupInfo> newBrokerGroups = newBrokerCluster.getGroups();
        List<BrokerGroupInfo> mergedBrokerGroups = new ArrayList<>(newBrokerGroups.size());
        for (BrokerGroupInfo newBrokerGroup : newBrokerGroups) {
            BrokerGroupInfo oldBrokerGroup = oldBrokerCluster.getGroupByName(newBrokerGroup.getGroupName());

            // 新 brokerGroup
            if (oldBrokerGroup == null) {
                mergedBrokerGroups.add(newBrokerGroup);
                continue;
            }

            // 复用 circuitBreaker
            if (Objects.equals(oldBrokerGroup.getMaster(), newBrokerGroup.getMaster())) {
                BrokerGroupInfo copy = new BrokerGroupInfo(newBrokerGroup.getGroupIndex(),
                        newBrokerGroup.getGroupName(), newBrokerGroup.getMaster(), newBrokerGroup.getSlaves(),
                        oldBrokerGroup.getCircuitBreaker());
                mergedBrokerGroups.add(copy);
                continue;
            }

            // 同名 BrokerGroup 但是 Master 发生变化
            mergedBrokerGroups.add(newBrokerGroup);
        }

        BrokerClusterInfo updatedCluster = new BrokerClusterInfo(mergedBrokerGroups);
        oldRef.set(updatedCluster);
    }

    private boolean isBrokerClusterEquals(BrokerClusterInfo oldClusterInfo, BrokerClusterInfo newClusterInfo) {
        List<BrokerGroupInfo> newBrokerGroups = newClusterInfo.getGroups();
        if (newBrokerGroups.size() != oldClusterInfo.getGroups().size()) {
            return false;
        }

        for (BrokerGroupInfo newBrokerGroup : newBrokerGroups) {
            BrokerGroupInfo oldBrokerGroup = oldClusterInfo.getGroupByName(newBrokerGroup.getGroupName());
            if (oldBrokerGroup == null) {
                return false;
            }

            if (!isBrokerGroupEquals(oldBrokerGroup, newBrokerGroup)) {
                return false;
            }
        }
        return true;
    }

    private boolean isBrokerGroupEquals(BrokerGroupInfo oldBrokerGroup, BrokerGroupInfo newBrokerGroup) {
        return Objects.equals(oldBrokerGroup.getMaster(), newBrokerGroup.getMaster()) &&
                Objects.equals(oldBrokerGroup.getSlaves(), newBrokerGroup.getSlaves()) &&
                Objects.equals(oldBrokerGroup.getGroupName(), newBrokerGroup.getGroupName()) &&
                Objects.equals(oldBrokerGroup.getGroupIndex(), newBrokerGroup.getGroupIndex()) &&
                Objects.equals(oldBrokerGroup.isAvailable(), newBrokerGroup.isAvailable());
    }

    private void logMetaInfo(MetaInfo metaInfo) {
        String msg = "meta server return empty broker, will retry in a few seconds. subject={}, client={}";
        LOGGER.error(msg, metaInfo.getSubject(), metaInfo.getClientType());
    }

    private boolean isEmptyCluster(MetaInfo metaInfo) {
        return metaInfo.getClientType() != ClientType.CONSUMER
                && metaInfo.getClusterInfo().getGroups().isEmpty();
    }

    @Override
    public BrokerClusterInfo getProducerBrokerCluster(ClientType clientType, String subject) {
        return getBrokerCluster(clientType, subject, "", false, false);
    }

    @Override
    public BrokerClusterInfo getConsumerBrokerCluster(ClientType clientType, String subject) {
        String key = buildBrokerClusterKey(clientType, subject);
        SettableFuture<AtomicReference<BrokerClusterInfo>> future = clusterMap.get(key);
        Preconditions.checkNotNull(future, "broker 信息不存在 %s %s", clientType, subject);
        try {
            return future.get().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BrokerClusterInfo getBrokerCluster(ClientType clientType, String subject, String consumerGroup,
            boolean isBroadcast, boolean isOrdered) {
        // 这个key上加group不兼容MetaInfoResponse
        String key = buildBrokerClusterKey(clientType, subject);
        SettableFuture<AtomicReference<BrokerClusterInfo>> future = clusterMap.computeIfAbsent(key, k -> {
            SettableFuture<AtomicReference<BrokerClusterInfo>> f = SettableFuture.create();
            registerHeartbeat(clientType, subject, consumerGroup, isBroadcast, isOrdered);
            return f;
        });
        try {
            return future.get().get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static String buildBrokerClusterKey(ClientType clientType, String subject) {
        return clientType.name() + ":" + subject;
    }

    @Override
    public void refreshMetaInfo(ClientType clientType, String subject) {
        refreshMetaInfo(clientType, subject, "");
    }

    @Override
    public void refreshMetaInfo(ClientType clientType, String subject, String consumerGroup) {
        metaInfoService.triggerHeartbeat(clientType.getCode(), subject, consumerGroup);
    }

    @Override
    public void releaseLock(String subject, String consumerGroup, String partitionName, String brokerGroupName,
            ConsumeStrategy consumeStrategy) {
        operateLock(CommandCode.RELEASE_PULL_LOCK, subject, consumerGroup, partitionName, brokerGroupName,
                consumeStrategy);
    }

    private void operateLock(short commandCode, String subject, String consumerGroup, String partitionName,
            String brokerGroupName, ConsumeStrategy consumeStrategy) {
        if (!Objects.equals(consumeStrategy, ConsumeStrategy.EXCLUSIVE)) {
            return;
        }
        BrokerClusterInfo brokerCluster = getConsumerBrokerCluster(ClientType.CONSUMER, subject);
        BrokerGroupInfo brokerGroup = brokerCluster.getGroupByName(brokerGroupName);
        LockOperationRequest request = new LockOperationRequest(partitionName, consumerGroup, clientId);
        try {
            nettyClient.sendAsync(brokerGroup.getMaster(),
                    RemotingBuilder.buildRequestDatagram(commandCode, new PayloadHolder() {
                        @Override
                        public void writeBody(ByteBuf out) {
                            Serializer<LockOperationRequest> serializer = Serializers
                                    .getSerializer(LockOperationRequest.class);
                            serializer.serialize(request, out, RemotingHeader.getCurrentVersion());
                        }
                    }), 3000);
        } catch (ClientSendException e) {
            LOGGER.error("锁操作失败 opCode {} broker {} subject {} partition {} consumerGroup {}", commandCode,
                    brokerGroupName, subject, partitionName, consumerGroup, e);
        }
    }


    @Override
    public String getAppCode() {
        return appCode;
    }

    private void updatePartitionCache(MetaInfoResponse response) {
        ClientType clientType = ClientType.of(response.getClientTypeCode());
        String subject = response.getSubject();
        if (clientType.isProducer()) {
            String key = createProducerPartitionMappingKey(ClientType.of(response.getClientTypeCode()), subject);
            ProducerMetaInfoResponse producerResponse = (ProducerMetaInfoResponse) response;
            updatePartitionCache(key, producerAllocationMap, producerResponse.getProducerAllocation());
        }

    }

    private <T extends Versionable> void updatePartitionCache(String key,
            Map<String, SettableFuture<AtomicReference<T>>> map,
            T newVal) {
        if (newVal == null) {
            return;
        }
        synchronized (key.intern()) {
            SettableFuture<AtomicReference<T>> future = map.computeIfAbsent(key, k -> SettableFuture.create());
            if (!future.isDone()) {
                future.set(new AtomicReference<>(newVal));
            } else {
                try {
                    AtomicReference<T> ref = future.get();
                    T oldVal = ref.get();
                    int oldVersion = oldVal.getVersion();
                    int newVersion = newVal.getVersion();
                    if (newVersion > oldVersion) {
                        ref.set(newVal);
                    }
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }
        }
    }

    private String createProducerPartitionMappingKey(ClientType clientType, String subject) {
        return clientType.name() + ":" + subject;
    }

    private MetaInfo parseResponse(MetaInfoResponse response) {
        if (response == null) {
            return null;
        }

        String subject = response.getSubject();
        if (Strings.isNullOrEmpty(subject)) {
            return null;
        }

        ClientType clientType = parseClientType(response);
        if (clientType == null) {
            return null;
        }

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
        SettableFuture<AtomicReference<ProducerAllocation>> future = producerAllocationMap
                .computeIfAbsent(producerKey, key -> SettableFuture.create());
        registerHeartbeat(clientType, subject, "", false, false);
        try {
            return future.get().get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void registerHeartbeat(ClientType clientType, String subject, String consumerGroup, boolean isBroadcast,
            boolean isOrdered) {
        metaInfoService.registerHeartbeat(
                appCode,
                clientType.getCode(),
                subject,
                consumerGroup,
                isBroadcast,
                isOrdered
        );
    }

}
