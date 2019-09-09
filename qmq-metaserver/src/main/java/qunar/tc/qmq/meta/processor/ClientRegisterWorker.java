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
import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.PartitionAllocation;
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
import qunar.tc.qmq.meta.order.DefaultPartitionService;
import qunar.tc.qmq.meta.order.PartitionService;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.utils.ClientLogUtils;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.RetrySubjectUtils;
import qunar.tc.qmq.utils.SubjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private final PartitionService partitionService;

    ClientRegisterWorker(final SubjectRouter subjectRouter, final CachedOfflineStateManager offlineStateManager, final Store store, CachedMetaInfoManager cachedMetaInfoManager) {
        this.subjectRouter = subjectRouter;
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.actorSystem = new ActorSystem("qmq_meta");
        this.offlineStateManager = offlineStateManager;
        this.store = store;
        this.partitionService = DefaultPartitionService.getInstance();

        VersionableComponentManager.registerComponent(BrokerFilter.class, Range.closedOpen(Integer.MIN_VALUE, (int) RemotingHeader.VERSION_10), new DefaultBrokerFilter(cachedMetaInfoManager));
        VersionableComponentManager.registerComponent(BrokerFilter.class, Range.closedOpen((int) RemotingHeader.VERSION_10, Integer.MAX_VALUE), new BrokerFilterV10());

        VersionableComponentManager.registerComponent(ResponseBuilder.class, Range.closedOpen(Integer.MIN_VALUE, (int) RemotingHeader.VERSION_10), new DefaultResponseBuilder());
        VersionableComponentManager.registerComponent(ResponseBuilder.class, Range.closedOpen((int) RemotingHeader.VERSION_10, Integer.MAX_VALUE), new ResponseBuilderV10());
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

    private MetaInfoResponse handleClientRegister(RemotingHeader header, MetaInfoRequest request) {
        String realSubject = RetrySubjectUtils.getRealSubject(request.getSubject());
        int clientRequestType = request.getRequestType();
        OnOfflineState onOfflineState = request.getOnlineState();

        if (SubjectUtils.isInValid(realSubject)) {
            onOfflineState = OnOfflineState.OFFLINE;
            return buildResponse(header, request, -2, onOfflineState, new BrokerCluster(new ArrayList<>()));
        }

        try {
            if (ClientRequestType.ONLINE.getCode() == clientRequestType) {
                store.insertClientMetaInfo(request);
            }

            final List<BrokerGroup> brokerGroups = subjectRouter.route(realSubject, request);
            BrokerFilter brokerFilter = VersionableComponentManager.getComponent(BrokerFilter.class, header.getVersion());
            final List<BrokerGroup> filteredBrokerGroups = brokerFilter.filter(realSubject, request.getClientTypeCode(), brokerGroups);
            final OnOfflineState clientState = offlineStateManager.queryClientState(request.getClientId(), request.getSubject(), request.getConsumerGroup());

            ClientLogUtils.log(realSubject,
                    "client register response, request:{}, realSubject:{}, brokerGroups:{}, clientState:{}",
                    request, realSubject, filteredBrokerGroups, clientState);

            return buildResponse(header, request, offlineStateManager.getLastUpdateTimestamp(), clientState, new BrokerCluster(filteredBrokerGroups));
        } catch (Exception e) {
            LOG.error("process exception. {}", request, e);
            onOfflineState = OnOfflineState.OFFLINE;
            return buildResponse(header, request, -2, onOfflineState, new BrokerCluster(new ArrayList<>()));
        } finally {
            if (ClientRequestType.HEARTBEAT.getCode() == clientRequestType || ClientRequestType.SWITCH_STATE.getCode() == clientRequestType) {
                // 上线的时候如果出现异常可能会将客户端上线状态改为下线
                request.setOnlineState(onOfflineState);
                EventDispatcher.dispatch(request);
            }
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
        if (version >= RemotingHeader.VERSION_10) {
            return new MetaInfoResponsePayloadHolderV10(response);
        } else {
            return new MetaInfoResponsePayloadHolder(response);
        }

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
            ClientType clientType = ClientType.of(request.getRequestType());
            String subject = request.getSubject();
            String consumerGroup = request.getConsumerGroup();
            int clientTypeCode = request.getClientTypeCode();
            if (clientType.isProducer()) {
                // 从数据库缓存中获取分区信息
                ProducerAllocation producerAllocation = cachedMetaInfoManager.getProducerAllocation(clientType, subject);
                if (producerAllocation == null) {
                    // 没有分区信息, 自动根据 subject-broker 信息创建一个零时的虚拟分区
                    producerAllocation = partitionService.getDefaultProducerAllocation(subject, brokerCluster);
                }
                return new ProducerMetaInfoResponse(updateTime, subject, consumerGroup, clientState, clientTypeCode, brokerCluster, producerAllocation);
            } else {
                PartitionAllocation partitionAllocation = partitionService.getActivatedPartitionAllocation(subject, consumerGroup);
                Map<String, Set<Integer>> clientId2PhysicalPartitions = partitionAllocation.getAllocationDetail().getClientId2PhysicalPartitions();
                Set<Integer> physicalPartitions = clientId2PhysicalPartitions.get(request.getClientId());
                ConsumeMode consumeMode = getConsumeMode(request);
                // TODO(zhenwei.liu) partition 分配, 需要根据 consumer 是否是独占消费来分配返回的 broker
                ConsumerAllocation consumerAllocation = new ConsumerAllocation(partitionAllocation.getVersion(), physicalPartitions, partitionService.getExpiredMills(System.currentTimeMillis()), consumeMode);
                return new ConsumerMetaInfoResponse(updateTime, subject, consumerGroup, clientState, clientTypeCode, brokerCluster, consumerAllocation);
            }
        }
    }

    private ConsumeMode getConsumeMode(MetaInfoRequest request) {
        if (request.isBroadcast()) {
            return ConsumeMode.EXCLUSIVE;
        }
        return request.getConsumeMode();
    }


    private static class MetaInfoResponsePayloadHolder implements PayloadHolder {

        private final MetaInfoResponse response;

        MetaInfoResponsePayloadHolder(MetaInfoResponse response) {
            this.response = response;
        }

        @Override
        public void writeBody(ByteBuf out) {
            Serializer<MetaInfoResponse> serializer = Serializers.getSerializer(MetaInfoResponse.class);
            serializer.serialize(response, out);
        }

        public MetaInfoResponse getResponse() {
            return response;
        }
    }

    private static class MetaInfoResponsePayloadHolderV10 extends MetaInfoResponsePayloadHolder implements PayloadHolder {

        MetaInfoResponsePayloadHolderV10(MetaInfoResponse response) {
            super(response);
        }

        @Override
        public void writeBody(ByteBuf out) {
            MetaInfoResponse response = getResponse();
            if (response instanceof ProducerMetaInfoResponse) {
                Serializer<ProducerMetaInfoResponse> serializer = Serializers.getSerializer(ProducerMetaInfoResponse.class);
                serializer.serialize((ProducerMetaInfoResponse) response, out);
            } else if (response instanceof ConsumerMetaInfoResponse) {
                Serializer<ConsumerMetaInfoResponse> serializer = Serializers.getSerializer(ConsumerMetaInfoResponse.class);
                serializer.serialize((ConsumerMetaInfoResponse) response, out);
            }
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
