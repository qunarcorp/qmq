package qunar.tc.qmq.codec;

import com.google.common.collect.RangeMap;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.PartitionInfo;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class PartitionInfoSerializer extends ObjectSerializer<PartitionInfo> {
    @Override
    void doSerialize(PartitionInfo partitionInfo, ByteBuf buf) {

        PayloadHolderUtils.writeString(partitionInfo.getSubject(), buf);

        buf.writeInt(partitionInfo.getVersion());
        buf.writeInt(partitionInfo.getLogicalPartitionNum());

        Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);
        rangeMapSerializer.serialize(partitionInfo.getLogical2PhysicalPartition(), buf);

        Serializer<Map> mapSerializer = Serializers.getSerializer(Map.class);
        mapSerializer.serialize(partitionInfo.getPhysicalPartition2Broker(), buf);
        mapSerializer.serialize(partitionInfo.getPhysicalPartition2DelayBroker(), buf);
    }

    @Override
    PartitionInfo doDeserialize(ByteBuf buf, Class... classes) {
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setSubject(PayloadHolderUtils.readString(buf));
        partitionInfo.setVersion(buf.readInt());
        partitionInfo.setLogicalPartitionNum(buf.readInt());

        Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);
        partitionInfo.setLogical2PhysicalPartition(rangeMapSerializer.deserialize(buf, Integer.class, Integer.class));

        Serializer<Map> mapSerializer = Serializers.getSerializer(Map.class);
        partitionInfo.setPhysicalPartition2Broker(mapSerializer.deserialize(buf, Integer.class, String.class));
        partitionInfo.setPhysicalPartition2DelayBroker(mapSerializer.deserialize(buf, Integer.class, String.class));

        return partitionInfo;
    }
}
