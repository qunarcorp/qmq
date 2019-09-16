package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class ConsumerMetaInfoResponseSerializer extends MetaInfoResponseSerializer {

    @Override
    void doSerialize(MetaInfoResponse response, ByteBuf out) {
        super.doSerialize(response, out);
        ConsumerMetaInfoResponse consumerResponse = (ConsumerMetaInfoResponse) response;
        Serializer<ConsumerAllocation> serializer = Serializers.getSerializer(ConsumerAllocation.class);
        serializer.serialize(consumerResponse.getConsumerAllocation(), out);
    }

    @Override
    MetaInfoResponse doDeserialize(ByteBuf buf, Type type) {
        MetaInfoResponse response = super.doDeserialize(buf, type);
        Serializer<ConsumerAllocation> serializer = Serializers.getSerializer(ConsumerAllocation.class);
        ConsumerAllocation consumerAllocation = serializer.deserialize(buf, null);
        return new ConsumerMetaInfoResponse(
                response.getTimestamp(),
                response.getSubject(),
                response.getConsumerGroup(),
                response.getOnOfflineState(),
                response.getClientTypeCode(),
                response.getBrokerCluster(),
                consumerAllocation);
    }
}
