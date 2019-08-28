package qunar.tc.qmq.codec;

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class PartitionSerializer extends ObjectSerializer<Partition> {

    private ParameterizedType rangeType = Types.newParamterizedType(null, Range.class, new Type[]{
            Integer.class
    });

    private Serializer<Range> rangeSerializer = getSerializer(Range.class);

    @Override
    void doSerialize(Partition partition, ByteBuf buf) {
        PayloadHolderUtils.writeString(partition.getSubject(), buf);
        buf.writeInt(partition.getPhysicalPartition());
        rangeSerializer.serialize(partition.getLogicalPartition(), buf);
        PayloadHolderUtils.writeString(partition.getBrokerGroup(), buf);
        PayloadHolderUtils.writeString(partition.getStatus().name(), buf);
    }

    @Override
    Partition doDeserialize(ByteBuf buf, Type type) {
        Partition partition = new Partition();
        partition.setSubject(PayloadHolderUtils.readString(buf));
        partition.setPhysicalPartition(buf.readInt());
        partition.setLogicalPartition(rangeSerializer.deserialize(buf, rangeType));
        partition.setBrokerGroup(PayloadHolderUtils.readString(buf));
        partition.setStatus(Partition.Status.valueOf(PayloadHolderUtils.readString(buf)));
        return partition;
    }
}
