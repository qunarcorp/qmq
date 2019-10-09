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

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
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
import qunar.tc.qmq.event.EventDispatcher;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.cache.CachedOfflineStateManager;
import qunar.tc.qmq.meta.order.AllocationService;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.utils.ClientLogUtils;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.SubjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author yunfeng.yang
 * @since 2017/9/1
 */
class ClientRegisterWorker implements ActorSystem.Processor<ClientRegisterProcessor.ClientRegisterMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRegisterWorker.class);

    private final SubjectRouter subjectRouter;
    private final ActorSystem actorSystem;
    private final Store store;
    private final CachedOfflineStateManager offlineStateManager;
    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final AllocationService allocationService;

    ClientRegisterWorker(final SubjectRouter subjectRouter, final CachedOfflineStateManager offlineStateManager, final Store store, CachedMetaInfoManager cachedMetaInfoManager, AllocationService allocationService) {
        this.subjectRouter = subjectRouter;
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.allocationService = allocationService;
        this.actorSystem = new ActorSystem("qmq_meta");
        this.offlineStateManager = offlineStateManager;
        this.store = store;

        VersionableComponentManager.registerComponent(BrokerFilter.class, Range.closedOpen(Integer.MIN_VALUE, (int) RemotingHeader.getOrderedMessageVersion()), new DefaultBrokerFilter(cachedMetaInfoManager));
        VersionableComponentManager.registerComponent(BrokerFilter.class, Range.closedOpen((int) RemotingHeader.getOrderedMessageVersion(), Integer.MAX_VALUE), new BrokerFilterV10());

        VersionableComponentManager.registerComponent(ResponseBuilder.class, Range.closedOpen(Integer.MIN_VALUE, (int) RemotingHeader.getOrderedMessageVersion()), new DefaultResponseBuilder());
        VersionableComponentManager.registerComponent(ResponseBuilder.class, Range.closedOpen((int) RemotingHeader.getOrderedMessageVersion(), Integer.MAX_VALUE), new ResponseBuilderV10());
    }

    void register(ClientRegisterProcessor.ClientRegisterMessage message) {
        actorSystem.dispatch("client_register_" + message.getMetaInfoRequest().getSubject(), message, this);
    }

    @Override
    public boolean process(ClientRegisterProcessor.ClientRegisterMessage message, ActorSystem.Actor<ClientRegisterProcessor.ClientRegisterMessage> self) {
        final MetaInfoRequest request = message.getMetaInfoRequest();

        final MetaInfoResponse response = handleClientRegister(message.getHeader(), request);
        writeResponse(message, response);
        return true;
    }


    // TODO(zhenwei.liu) 这里需要根据 Producer 还是 Consumer 来决定是否返回老分区, 对 Producer 不返回老分区, 对 Consumer 则返回所有
    private MetaInfoResponse handleClientRegister(RemotingHeader header, MetaInfoRequest request) {
        int clientRequestType = request.getRequestType();

        String subject = request.getSubject();
        if (SubjectUtils.isInValid(subject)) {
            return buildResponse(header, request, -2, OnOfflineState.OFFLINE, new BrokerCluster(new ArrayList<>()));
        }

        try {
            if (ClientRequestType.ONLINE.getCode() == clientRequestType) {
                store.insertClientMetaInfo(request);
            }

            final List<BrokerGroup> brokerGroups = subjectRouter.route(subject, request.getClientTypeCode());
            BrokerFilter brokerFilter = VersionableComponentManager.getComponent(BrokerFilter.class, header.getVersion());
            final List<BrokerGroup> filteredBrokerGroups = brokerFilter.filter(subject, request.getClientTypeCode(), brokerGroups);
            OnOfflineState onlineState = offlineStateManager.queryClientState(request.getClientId(), request.getSubject(), request.getConsumerGroup());

            ClientLogUtils.log(subject,
                    "client register response, request:{}, realSubject:{}, brokerGroups:{}, clientState:{}",
                    request, subject, filteredBrokerGroups, onlineState);

            return buildResponse(header, request, offlineStateManager.getLastUpdateTimestamp(), onlineState, new BrokerCluster(filteredBrokerGroups));
        } catch (Exception e) {
            LOG.error("onSuccess exception. {}", request, e);
            return buildResponse(header, request, -2, OnOfflineState.OFFLINE, new BrokerCluster(new ArrayList<>()));
        } finally {
            // 上线的时候如果出现异常可能会将客户端上线状态改为下线
            EventDispatcher.dispatch(request);
        }
    }

    private MetaInfoResponse buildResponse(RemotingHeader header, MetaInfoRequest request, long updateTime, OnOfflineState clientState, BrokerCluster brokerCluster) {
        short version = header.getVersion();
        ResponseBuilder component = VersionableComponentManager.getComponent(ResponseBuilder.class, version);
        return component.buildResponse(request, updateTime, clientState, brokerCluster);
    }

    private void writeResponse(final ClientRegisterProcessor.ClientRegisterMessage message, final MetaInfoResponse response) {
        final RemotingHeader header = message.getHeader();
        final PayloadHolder payloadHolder = createPayloadHolder(message, response);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, header, payloadHolder);
        message.getCtx().writeAndFlush(datagram);
    }

    private PayloadHolder createPayloadHolder(final ClientRegisterProcessor.ClientRegisterMessage message, final MetaInfoResponse response) {
        RemotingHeader header = message.getHeader();
        short version = header.getVersion();
        return out -> {
            Serializer<MetaInfoResponse> serializer = Serializers.getSerializer(MetaInfoResponse.class);
            serializer.serialize(response, out, version);
        };
    }

    private interface ResponseBuilder {

        MetaInfoResponse buildResponse(MetaInfoRequest clientRequest, long updateTime, OnOfflineState clientState, BrokerCluster brokerCluster);
    }

    private class DefaultResponseBuilder implements ResponseBuilder {

        @Override
        public MetaInfoResponse buildResponse(MetaInfoRequest request, long updateTime, OnOfflineState clientState, BrokerCluster brokerCluster) {
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
        public MetaInfoResponse buildResponse(MetaInfoRequest request, long updateTime, OnOfflineState clientState, BrokerCluster brokerCluster) {
            int clientTypeCode = request.getClientTypeCode();
            ClientType clientType = ClientType.of(clientTypeCode);
            String subject = request.getSubject();
            String consumerGroup = request.getConsumerGroup();
            if (clientType.isProducer()) {
                // 从数据库缓存中获取分区信息
                ProducerAllocation producerAllocation = allocationService.getProducerAllocation(clientType, subject, brokerCluster.getBrokerGroups());
                return new ProducerMetaInfoResponse(updateTime, subject, consumerGroup, clientState, clientTypeCode, brokerCluster, producerAllocation);
            } else if (clientType.isConsumer()) {
                String clientId = request.getClientId();
                ConsumeStrategy consumeStrategy = ConsumeStrategy.getConsumeStrategy(request.isBroadcast(), request.isOrdered());
                ConsumerAllocation consumerAllocation = allocationService.getConsumerAllocation(subject, consumerGroup, clientId, consumeStrategy, brokerCluster.getBrokerGroups());
                return new ConsumerMetaInfoResponse(updateTime, subject, consumerGroup, clientState, clientTypeCode, brokerCluster, consumerAllocation);
            }
            throw new UnsupportedOperationException("客户端类型不匹配");
        }
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

        public List<BrokerGroup> filterReadonlyBrokerGroup(String realSubject, int clientTypeCode, List<BrokerGroup> brokerGroups) {
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
