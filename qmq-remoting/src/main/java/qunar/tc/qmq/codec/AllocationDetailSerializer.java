package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.PartitionProps;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class AllocationDetailSerializer extends ObjectSerializer<PartitionAllocation.AllocationDetail> {

    private ParameterizedType mapType = Types.newParameterizedType(null, Map.class, new Type[] {
       String.class,
       Types.newParameterizedType(null, Set.class, new Type[] {
               PartitionProps.class
       })
    });
    private Serializer<Map> mapSerializer = getSerializer(mapType);

    @Override
    void doSerialize(PartitionAllocation.AllocationDetail allocationDetail, ByteBuf buf) {
        Map<String, Set<PartitionProps>> clientId2SubjectLocation = allocationDetail.getClientId2SubjectLocation();
        mapSerializer.serialize(clientId2SubjectLocation, buf);
    }

    @Override
    PartitionAllocation.AllocationDetail doDeserialize(ByteBuf buf, Type type) {
        PartitionAllocation.AllocationDetail allocationDetail = new PartitionAllocation.AllocationDetail();
        allocationDetail.setClientId2SubjectLocation(mapSerializer.deserialize(buf, mapType));
        return allocationDetail;
    }
}
