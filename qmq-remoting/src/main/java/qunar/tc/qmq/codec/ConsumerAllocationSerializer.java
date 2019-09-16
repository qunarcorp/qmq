package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;
import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ConsumerAllocationSerializer extends ObjectSerializer<ConsumerAllocation> {

    private static final Type subjectLocationListType = Types.newParameterizedType(null, List.class, new Type[]{PartitionProps.class});

    @Override
    void doSerialize(ConsumerAllocation consumerAllocation, ByteBuf buf) {
        buf.writeInt(consumerAllocation.getVersion());
        buf.writeLong(consumerAllocation.getExpired());
        PayloadHolderUtils.writeString(consumerAllocation.getConsumeStrategy().name(), buf);
        List<PartitionProps> partitionProps = consumerAllocation.getPartitionProps();
        Serializer<List> serializer = getSerializer(List.class);
        serializer.serialize(partitionProps, buf);
    }

    @Override
    ConsumerAllocation doDeserialize(ByteBuf buf, Type type) {
        int allocationVersion = buf.readInt();
        long expired = buf.readLong();
        ConsumeStrategy consumeStrategy = ConsumeStrategy.valueOf(PayloadHolderUtils.readString(buf));
        Serializer<List> serializer = getSerializer(List.class);
        List<PartitionProps> partitionProps = serializer.deserialize(buf, subjectLocationListType);
        return new ConsumerAllocation(allocationVersion, partitionProps, expired, consumeStrategy);
    }
}
