package qunar.tc.qmq.codec;

import com.google.common.collect.RangeMap;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class PartitionMappingSerializer extends ObjectSerializer<PartitionMapping> {

    private ParameterizedType rangeMapType = Types.newParamterizedType(null, RangeMap.class, new Type[]{
            Integer.class,
            Partition.class
    });

    private Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);

    @Override
    void doSerialize(PartitionMapping partitionMapping, ByteBuf buf) {
        PayloadHolderUtils.writeString(partitionMapping.getSubject(), buf);
        buf.writeInt(partitionMapping.getLogicalPartitionNum());
        buf.writeInt(partitionMapping.getVersion());
        rangeMapSerializer.serialize(partitionMapping.getLogical2PhysicalPartition(), buf);
    }

    @Override
    PartitionMapping doDeserialize(ByteBuf buf, Type type) {
        PartitionMapping partitionMapping = new PartitionMapping();
        partitionMapping.setSubject(PayloadHolderUtils.readString(buf));
        partitionMapping.setLogicalPartitionNum(buf.readInt());
        partitionMapping.setVersion(buf.readInt());
        partitionMapping.setLogical2PhysicalPartition(rangeMapSerializer.deserialize(buf, rangeMapType));
        return partitionMapping;
    }
}
