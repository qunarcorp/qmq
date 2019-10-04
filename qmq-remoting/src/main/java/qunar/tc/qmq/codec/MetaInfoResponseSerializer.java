package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
class MetaInfoResponseSerializer extends ObjectSerializer<MetaInfoResponse> {

    @Override
    void doSerialize(MetaInfoResponse response, ByteBuf out) {
        out.writeLong(response.getTimestamp());
        PayloadHolderUtils.writeString(response.getSubject(), out);
        PayloadHolderUtils.writeString(response.getConsumerGroup(), out);
        out.writeByte(response.getOnOfflineState().code());
        out.writeByte(response.getClientTypeCode());
        out.writeShort(response.getBrokerCluster().getBrokerGroups().size());
        writeBrokerCluster(response, out);

        if (response instanceof ProducerMetaInfoResponse) {
            ProducerMetaInfoResponse producerResponse = (ProducerMetaInfoResponse) response;
            ProducerAllocation partitionMapping = producerResponse.getProducerAllocation();
            Serializer<ProducerAllocation> serializer = Serializers.getSerializer(ProducerAllocation.class);
            serializer.serialize(partitionMapping, out);
        } else if (response instanceof ConsumerMetaInfoResponse) {
            ConsumerMetaInfoResponse consumerResponse = (ConsumerMetaInfoResponse) response;
            Serializer<ConsumerAllocation> serializer = Serializers.getSerializer(ConsumerAllocation.class);
            serializer.serialize(consumerResponse.getConsumerAllocation(), out);
        }
    }

    private void writeBrokerCluster(MetaInfoResponse response, ByteBuf out) {
        for (BrokerGroup brokerGroup : response.getBrokerCluster().getBrokerGroups()) {
            PayloadHolderUtils.writeString(brokerGroup.getGroupName(), out);
            PayloadHolderUtils.writeString(brokerGroup.getMaster(), out);
            out.writeLong(brokerGroup.getUpdateTime());
            out.writeByte(brokerGroup.getBrokerState().getCode());
        }
    }

    @Override
    MetaInfoResponse doDeserialize(ByteBuf buf, Type type) {
        long timestamp = buf.readLong();
        String subject = PayloadHolderUtils.readString(buf);
        String consumerGroup = PayloadHolderUtils.readString(buf);
        OnOfflineState onOfflineState = OnOfflineState.fromCode(buf.readByte());
        byte clientTypeCode = buf.readByte();
        BrokerCluster brokerCluster = deserializeBrokerCluster(buf);

        if (clientTypeCode == ClientType.PRODUCER.getCode() || clientTypeCode == ClientType.DELAY_PRODUCER.getCode()) {
            Serializer<ProducerAllocation> serializer = Serializers.getSerializer(ProducerAllocation.class);
            ProducerAllocation producerAllocation = serializer.deserialize(buf, null);
            return new ProducerMetaInfoResponse(
                    timestamp,
                    subject,
                    consumerGroup,
                    onOfflineState,
                    clientTypeCode,
                    brokerCluster,
                    producerAllocation);
        } else if (clientTypeCode == ClientType.CONSUMER.getCode()) {
            Serializer<ConsumerAllocation> serializer = Serializers.getSerializer(ConsumerAllocation.class);
            ConsumerAllocation consumerAllocation = serializer.deserialize(buf, null);
            return new ConsumerMetaInfoResponse(
                    timestamp,
                    subject,
                    consumerGroup,
                    onOfflineState,
                    clientTypeCode,
                    brokerCluster,
                    consumerAllocation);
        }
        throw new IllegalArgumentException(String.format("无法识别的 response 类型", clientTypeCode));
    }

    private static BrokerCluster deserializeBrokerCluster(ByteBuf buf) {
        final int brokerGroupSize = buf.readShort();
        final List<BrokerGroup> brokerGroups = new ArrayList<>(brokerGroupSize);
        for (int i = 0; i < brokerGroupSize; i++) {
            final BrokerGroup brokerGroup = new BrokerGroup();
            brokerGroup.setGroupName(PayloadHolderUtils.readString(buf));
            brokerGroup.setMaster(PayloadHolderUtils.readString(buf));
            brokerGroup.setUpdateTime(buf.readLong());
            final int brokerStateCode = buf.readByte();
            final BrokerState brokerState = BrokerState.codeOf(brokerStateCode);
            brokerGroup.setBrokerState(brokerState);
            brokerGroups.add(brokerGroup);
        }
        return new BrokerCluster(brokerGroups);
    }
}
