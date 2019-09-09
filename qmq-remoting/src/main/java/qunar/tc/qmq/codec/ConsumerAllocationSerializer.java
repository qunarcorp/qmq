package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ConsumerAllocationSerializer extends ObjectSerializer<ConsumerAllocation> {

    private static final Type physicalPartitionsType = Types.newParameterizedType(null, Set.class, new Type[]{Integer.class});

    @Override
    void doSerialize(ConsumerAllocation consumerAllocation, ByteBuf buf) {
        buf.writeInt(consumerAllocation.getVersion());
        buf.writeLong(consumerAllocation.getExpired());
        PayloadHolderUtils.writeString(consumerAllocation.getConsumeMode().name(), buf);
        Set<Integer> physicalPartitions = consumerAllocation.getPhysicalPartitions();
        Serializer<Set> serializer = getSerializer(physicalPartitions.getClass());
        serializer.serialize(physicalPartitions, buf);
    }

    @Override
    ConsumerAllocation doDeserialize(ByteBuf buf, Type type) {
        int allocationVersion = buf.readInt();
        long expired = buf.readLong();
        ConsumeMode consumeMode = ConsumeMode.valueOf(PayloadHolderUtils.readString(buf));
        Serializer<Set> serializer = getSerializer(Set.class);
        Set<Integer> physicalPartitions = serializer.deserialize(buf, physicalPartitionsType);
        return new ConsumerAllocation(allocationVersion, physicalPartitions, expired, consumeMode);
    }
}
