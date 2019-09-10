package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.meta.SubjectLocation;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;
import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ConsumerAllocationSerializer extends ObjectSerializer<ConsumerAllocation> {

    private static final Type subjectLocationListType = Types.newParameterizedType(null, List.class, new Type[]{SubjectLocation.class});

    @Override
    void doSerialize(ConsumerAllocation consumerAllocation, ByteBuf buf) {
        buf.writeInt(consumerAllocation.getVersion());
        buf.writeLong(consumerAllocation.getExpired());
        PayloadHolderUtils.writeString(consumerAllocation.getConsumeMode().name(), buf);
        List<SubjectLocation> subjectLocations = consumerAllocation.getSubjectLocations();
        Serializer<List> serializer = getSerializer(List.class);
        serializer.serialize(subjectLocations, buf);
    }

    @Override
    ConsumerAllocation doDeserialize(ByteBuf buf, Type type) {
        int allocationVersion = buf.readInt();
        long expired = buf.readLong();
        ConsumeMode consumeMode = ConsumeMode.valueOf(PayloadHolderUtils.readString(buf));
        Serializer<List> serializer = getSerializer(List.class);
        List<SubjectLocation> subjectLocations = serializer.deserialize(buf, subjectLocationListType);
        return new ConsumerAllocation(allocationVersion, subjectLocations, expired, consumeMode);
    }
}
