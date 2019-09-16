package qunar.tc.qmq.codec;

import com.google.common.collect.RangeMap;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ProducerAllocationSerializer extends ObjectSerializer<ProducerAllocation> {

    private ParameterizedType rangeMapType = Types.newParameterizedType(null, RangeMap.class, new Type[]{
            Integer.class,
            PartitionProps.class
    });

    private Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);

    @Override
    void doSerialize(ProducerAllocation producerAllocation, ByteBuf buf) {
        PayloadHolderUtils.writeString(producerAllocation.getSubject(), buf);
        buf.writeInt(producerAllocation.getVersion());
        rangeMapSerializer.serialize(producerAllocation.getLogical2SubjectLocation(), buf);
    }

    @Override
    ProducerAllocation doDeserialize(ByteBuf buf, Type type) {
        String subject = PayloadHolderUtils.readString(buf);
        int version = buf.readInt();
        RangeMap logical2SubjectLocation = rangeMapSerializer.deserialize(buf, rangeMapType);
        return new ProducerAllocation(subject, version, logical2SubjectLocation);
    }
}
