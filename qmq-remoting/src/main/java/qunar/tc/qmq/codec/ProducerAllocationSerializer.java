package qunar.tc.qmq.codec;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ProducerAllocationSerializer extends ObjectSerializer<ProducerAllocation> {

    private ParameterizedType rangeMapType = Types.newParameterizedType(null, RangeMap.class, new Type[]{
            Types.newParameterizedType(
                    null, Range.class, new Type[]{Integer.class}
            ),
            PartitionProps.class
    });

    @Override
    void doSerialize(ProducerAllocation producerAllocation, ByteBuf buf, short version) {
        PayloadHolderUtils.writeString(producerAllocation.getSubject(), buf);
        buf.writeInt(producerAllocation.getVersion());
        Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);
        rangeMapSerializer.serialize(producerAllocation.getLogical2SubjectLocation(), buf, version);
    }

    @Override
    ProducerAllocation doDeserialize(ByteBuf buf, Type type, short version) {
        String subject = PayloadHolderUtils.readString(buf);
        int allocationVersion = buf.readInt();
        Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);
        RangeMap logical2SubjectLocation = rangeMapSerializer.deserialize(buf, rangeMapType, version);
        return new ProducerAllocation(subject, allocationVersion, logical2SubjectLocation);
    }
}
