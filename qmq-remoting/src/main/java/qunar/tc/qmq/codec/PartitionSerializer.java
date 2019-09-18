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

    private ParameterizedType rangeType = Types.newParameterizedType(null, Range.class, new Type[]{
            Integer.class
    });

    private Serializer<Range> rangeSerializer = getSerializer(Range.class);

    @Override
    void doSerialize(Partition partition, ByteBuf buf) {
        PayloadHolderUtils.writeString(partition.getSubject(), buf);
        PayloadHolderUtils.writeString(partition.getPartitionName(), buf);
        buf.writeInt(partition.getPartitionId());
        rangeSerializer.serialize(partition.getLogicalPartition(), buf);
        PayloadHolderUtils.writeString(partition.getBrokerGroup(), buf);
        PayloadHolderUtils.writeString(partition.getStatus().name(), buf);
    }

    @Override
    Partition doDeserialize(ByteBuf buf, Type type) {
        String subject = PayloadHolderUtils.readString(buf);
        String partitionName = PayloadHolderUtils.readString(buf);
        int partitionId = buf.readInt();
        Range logicalRange = rangeSerializer.deserialize(buf, rangeType);
        String brokerGroup = PayloadHolderUtils.readString(buf);
        Partition.Status status = Partition.Status.valueOf(PayloadHolderUtils.readString(buf));
        return new Partition(
                subject,
                partitionName,
                partitionId,
                logicalRange,
                brokerGroup,
                status
        );
    }
}
