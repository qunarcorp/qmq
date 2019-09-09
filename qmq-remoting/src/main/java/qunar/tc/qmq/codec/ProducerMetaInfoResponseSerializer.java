package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class ProducerMetaInfoResponseSerializer extends MetaInfoResponseSerializer {

    @Override
    void doSerialize(MetaInfoResponse response, ByteBuf out) {
        super.doSerialize(response, out);
        ProducerMetaInfoResponse producerResponse = (ProducerMetaInfoResponse) response;
        ProducerAllocation partitionMapping = producerResponse.getProducerAllocation();
        Serializer<ProducerAllocation> serializer = Serializers.getSerializer(ProducerAllocation.class);
        serializer.serialize(partitionMapping, out);
    }

    @Override
    MetaInfoResponse doDeserialize(ByteBuf buf, Type type) {
        MetaInfoResponse response = super.doDeserialize(buf, type);
        Serializer<ProducerAllocation> serializer = Serializers.getSerializer(ProducerAllocation.class);
        ProducerAllocation producerAllocation = serializer.deserialize(buf, null);
        return new ProducerMetaInfoResponse(
                response.getTimestamp(),
                response.getSubject(),
                response.getConsumerGroup(),
                response.getOnOfflineState(),
                response.getClientTypeCode(),
                response.getBrokerCluster(),
                producerAllocation);

    }
}
