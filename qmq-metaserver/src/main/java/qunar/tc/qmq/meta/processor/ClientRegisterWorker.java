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

package qunar.tc.qmq.meta.processor;

import static qunar.tc.qmq.common.PartitionConstants.EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS;

import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.common.VersionableComponentManager;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.cache.CachedOfflineStateManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.order.AllocationService;
import qunar.tc.qmq.meta.order.ConsumerPartitionAllocator;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.utils.ClientLogUtils;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.RetryPartitionUtils;
import qunar.tc.qmq.utils.SubjectUtils;

/**
 * @author yunfeng.yang
 * @since 2017/9/1
 */
class ClientRegisterWorker implements ActorSystem.Processor<ClientRegisterProcessor.ClientRegisterMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientRegisterWorker.class);

    private final SubjectRouter subjectRouter;
    private final ActorSystem actorSystem;
    private final Store store;
    private final ClientMetaInfoStore clientMetaInfoStore;
    private final CachedOfflineStateManager offlineStateManager;
    private final AllocationService allocationService;
    private final ConsumerPartitionAllocator consumerPartitionAllocator;

    ClientRegisterWorker(final SubjectRouter subjectRouter, final CachedOfflineStateManager offlineStateManager,
            final Store store, CachedMetaInfoManager cachedMetaInfoManager, ClientMetaInfoStore clientMetaInfoStore,
            AllocationService allocationService, ConsumerPartitionAllocator consumerPartitionAllocator) {
        this.subjectRouter = subjectRouter;
        this.clientMetaInfoStore = clientMetaInfoStore;
        this.allocationService = allocationService;
        this.consumerPartitionAllocator = consumerPartitionAllocator;
        this.actorSystem = new ActorSystem("qmq_meta");
        this.offlineStateManager = offlineStateManager;
        this.store = store;

        VersionableComponentManager.registerComponent(BrokerFilter.class,
                Range.closedOpen(Integer.MIN_VALUE, (int) RemotingHeader.getOrderedMessageVersion()),
                new DefaultBrokerFilter(cachedMetaInfoManager));
        VersionableComponentManager.registerComponent(BrokerFilter.class,
                Range.closedOpen((int) RemotingHeader.getOrderedMessageVersion(), Integer.MAX_VALUE),
                new BrokerFilterV10());

        VersionableComponentManager.registerComponent(ResponseBuilder.class,
                Range.closedOpen(Integer.MIN_VALUE, (int) RemotingHeader.getOrderedMessageVersion()),
                new DefaultResponseBuilder());
        VersionableComponentManager.registerComponent(ResponseBuilder.class,
                Range.closedOpen((int) RemotingHeader.getOrderedMessageVersion(), Integer.MAX_VALUE),
                new ResponseBuilderV10());
    }

    void register(ClientRegisterProcessor.ClientRegisterMessage message) {
        actorSystem.dispatch("client_register_" + message.getMetaInfoRequest().getSubject(), message, this);
    }

    @Override
    public boolean process(ClientRegisterProcessor.ClientRegisterMessage message,
            ActorSystem.Actor<ClientRegisterProcessor.ClientRegisterMessage> self) {
        final MetaInfoRequest request = message.getMetaInfoRequest();

        final MetaInfoResponse response = handleClientRegister(message.getHeader(), request);
        writeResponse(message, response);
        return true;
    }


    private MetaInfoResponse handleClientRegister(RemotingHeader header, MetaInfoRequest request) {
        int clientRequestType = request.getRequestType();

        // 由于旧版本客户端还会发送 retry 的 subject 过来, 需要特殊处理
        String subject = RetryPartitionUtils.getRealPartitionName(request.getSubject());
        if (SubjectUtils.isInValid(subject)) {
            return buildResponse(header, request, -2, OnOfflineState.OFFLINE, new BrokerCluster(new ArrayList<>()),
                    ConsumeStrategy.SHARED, Long.MIN_VALUE);
        }

        try {
            if (ClientRequestType.ONLINE.getCode() == clientRequestType) {
                store.insertClientMetaInfo(request);
            }

            final List<BrokerGroup> brokerGroups = subjectRouter.route(subject, request.getClientTypeCode());
            BrokerFilter brokerFilter = VersionableComponentManager
                    .getComponent(BrokerFilter.class, header.getVersion());
            final List<BrokerGroup> filteredBrokerGroups = brokerFilter
                    .filter(subject, request.getClientTypeCode(), brokerGroups);
            OnOfflineState onlineState = offlineStateManager
                    .queryClientState(request.getClientId(), request.getSubject(), request.getConsumerGroup());

            ClientLogUtils.log(subject,
                    "client register response, request:{}, realSubject:{}, brokerGroups:{}, clientState:{}",
                    request, subject, filteredBrokerGroups, onlineState);

            ConsumeStrategy consumeStrategy = null;
            if (request.getClientTypeCode() == ClientType.CONSUMER.getCode()) {
                consumeStrategy = getConsumeStrategy(request);
                updateClientMetaInfo(request, consumeStrategy);
            }

            return buildResponse(header, request, offlineStateManager.getLastUpdateTimestamp(), onlineState,
                    new BrokerCluster(filteredBrokerGroups), consumeStrategy, expiredTimestamp(clientRequestType, request.getOnlineState().code()));
        } catch (Exception e) {
            LOGGER.error("handle client register exception. {} {} {}", request.getSubject(),
                    request.getClientTypeCode(), request.getClientId(), e);
            return buildResponse(header, request, -2, OnOfflineState.OFFLINE, new BrokerCluster(new ArrayList<>()),
                    ConsumeStrategy.SHARED, Long.MIN_VALUE);
        }
    }

    private void updateClientMetaInfo(MetaInfoRequest request, ConsumeStrategy consumeStrategy) {
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        int requestType = request.getRequestType();
        String clientId = request.getClientId();

        int clientTypeCode = request.getClientTypeCode();
        ClientType clientType = ClientType.of(clientTypeCode);

        if (Objects.equals(consumeStrategy, ConsumeStrategy.EXCLUSIVE) && clientType.isConsumer()) {

            // 更新在线状态
            ClientMetaInfo clientMetaInfo = new ClientMetaInfo(
                    subject,
                    clientTypeCode,
                    request.getAppCode(),
                    "",
                    clientId,
                    consumerGroup,
                    request.getOnlineState(),
                    consumeStrategy
            );
            clientMetaInfoStore.updateClientOnlineState(clientMetaInfo, consumeStrategy);

            if (ClientRequestType.SWITCH_STATE.getCode() == requestType) {
                // 触发重分配
                consumerPartitionAllocator.reallocate(subject, consumerGroup);
            }
        }
    }

    private ConsumeStrategy getConsumeStrategy(MetaInfoRequest request) {
        // 第一次上线判断旧的 consumer 的 consumeStrategy, 只能沿用老的策略
        String subject = request.getSubject();
        int requestType = request.getRequestType();
        String clientId = request.getClientId();
        String consumerGroup = request.getConsumerGroup();

        ConsumeStrategy consumeStrategy;
        if (requestType == ClientRequestType.ONLINE.getCode()) {
            ClientMetaInfo consumerState = clientMetaInfoStore.queryConsumer(subject, consumerGroup, clientId);
            if (consumerState != null && consumerState.getConsumeStrategy() != null) {
                consumeStrategy = consumerState.getConsumeStrategy();
            } else {
                consumeStrategy = ConsumeStrategy.getConsumeStrategy(request.isBroadcast(), request.isOrdered());
            }
        } else {
            // 使用客户端提供的参数
            consumeStrategy = request.getConsumeStrategy();
            if (consumeStrategy == null) {
                consumeStrategy = ConsumeStrategy.SHARED;
            }
        }

        return consumeStrategy;
    }

    private MetaInfoResponse buildResponse(RemotingHeader header, MetaInfoRequest request, long updateTime,
            OnOfflineState clientState, BrokerCluster brokerCluster, ConsumeStrategy consumeStrategy,
            long authExpireTime) {
        short version = header.getVersion();
        ResponseBuilder component = VersionableComponentManager.getComponent(ResponseBuilder.class, version);
        return component.buildResponse(request, updateTime, clientState, brokerCluster, consumeStrategy, authExpireTime);
    }

    private void writeResponse(final ClientRegisterProcessor.ClientRegisterMessage message,
            final MetaInfoResponse response) {
        final RemotingHeader header = message.getHeader();
        final PayloadHolder payloadHolder = createPayloadHolder(message, response);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, header, payloadHolder);
        message.getCtx().writeAndFlush(datagram);
    }

    private PayloadHolder createPayloadHolder(final ClientRegisterProcessor.ClientRegisterMessage message,
            final MetaInfoResponse response) {
        RemotingHeader header = message.getHeader();
        short version = header.getVersion();
        return out -> {
            Serializer<MetaInfoResponse> serializer = Serializers.getSerializer(MetaInfoResponse.class);
            serializer.serialize(response, out, version);
        };
    }

    private interface ResponseBuilder {

        MetaInfoResponse buildResponse(MetaInfoRequest clientRequest, long updateTime, OnOfflineState clientState,
                BrokerCluster brokerCluster, ConsumeStrategy consumeStrategy, long authExpireTime);
    }

    private class DefaultResponseBuilder implements ResponseBuilder {

        @Override
        public MetaInfoResponse buildResponse(MetaInfoRequest request, long updateTime, OnOfflineState clientState,
                BrokerCluster brokerCluster, ConsumeStrategy consumeStrategy, long authExpireTime) {
            return new MetaInfoResponse(
                    updateTime,
                    request.getSubject(),
                    request.getConsumerGroup(),
                    clientState,
                    request.getClientTypeCode(),
                    brokerCluster
            );
        }
    }

    private class ResponseBuilderV10 implements ResponseBuilder {

        @Override
        public MetaInfoResponse buildResponse(MetaInfoRequest request, long updateTime, OnOfflineState clientState,
                BrokerCluster brokerCluster, ConsumeStrategy consumeStrategy, long authExpireTime) {
            int clientTypeCode = request.getClientTypeCode();
            ClientType clientType = ClientType.of(clientTypeCode);
            String subject = request.getSubject();
            String realSubject = RetryPartitionUtils.getRealPartitionName(request.getSubject());
            String consumerGroup = request.getConsumerGroup();

            if (clientType.isProducer()) {
                ProducerAllocation producerAllocation = allocationService
                        .getProducerAllocation(clientType, realSubject, brokerCluster.getBrokerGroups());
                return new ProducerMetaInfoResponse(updateTime, subject, consumerGroup, clientState, clientTypeCode,
                        brokerCluster, producerAllocation);
            } else if (clientType.isConsumer()) {
                String clientId = request.getClientId();
                ConsumerAllocation consumerAllocation = allocationService
                        .getConsumerAllocation(realSubject, consumerGroup, clientId, authExpireTime, consumeStrategy,
                                brokerCluster.getBrokerGroups());
                return new ConsumerMetaInfoResponse(updateTime, subject, consumerGroup, clientState, clientTypeCode,
                        brokerCluster, consumerAllocation);
            }
            throw new UnsupportedOperationException("客户端类型不匹配");
        }
    }

    private long expiredTimestamp(int requestType, int onOfflineState) {
        if (requestType == ClientRequestType.SWITCH_STATE.getCode() && onOfflineState == OnOfflineState.OFFLINE.code()) {
            return Long.MIN_VALUE;
        }
        return System.currentTimeMillis() + EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS;
    }

    private interface BrokerFilter {

        List<BrokerGroup> filter(String subject, int clientTypeCode, List<BrokerGroup> brokerGroupInfo);
    }

    private static class DefaultBrokerFilter implements BrokerFilter {

        private CachedMetaInfoManager cachedMetaInfoManager;

        public DefaultBrokerFilter(CachedMetaInfoManager cachedMetaInfoManager) {
            this.cachedMetaInfoManager = cachedMetaInfoManager;
        }

        @Override
        public List<BrokerGroup> filter(String subject, int clientTypeCode, List<BrokerGroup> brokerGroups) {
            if (brokerGroups.isEmpty()) {
                return brokerGroups;
            }

            final List<BrokerGroup> result = new ArrayList<>();
            for (final BrokerGroup brokerGroup : brokerGroups) {
                if (brokerGroup.getBrokerState() != BrokerState.NRW) {
                    result.add(brokerGroup);
                }
            }

            return filterReadonlyBrokerGroup(subject, clientTypeCode, result);
        }

        public List<BrokerGroup> filterReadonlyBrokerGroup(String realSubject, int clientTypeCode,
                List<BrokerGroup> brokerGroups) {
            if (clientTypeCode != ClientType.PRODUCER.getCode()
                    && clientTypeCode != ClientType.DELAY_PRODUCER.getCode()) {
                return brokerGroups;
            }

            List<BrokerGroup> result = new ArrayList<>();
            for (BrokerGroup brokerGroup : brokerGroups) {
                if (isReadonlyForSubject(realSubject, brokerGroup.getGroupName())) {
                    continue;
                }

                result.add(brokerGroup);
            }

            return result;
        }

        private boolean isReadonlyForSubject(final String subject, final String brokerGroup) {
            final Set<String> subjects = cachedMetaInfoManager.getBrokerGroupReadonlySubjects(brokerGroup);
            if (subjects == null) {
                return false;
            }

            return subjects.contains(subject) || subjects.contains("*");
        }
    }

    private static class BrokerFilterV10 implements BrokerFilter {

        @Override
        public List<BrokerGroup> filter(String subject, int clientTypeCode, List<BrokerGroup> brokerGroups) {
            return brokerGroups;
        }
    }
}
