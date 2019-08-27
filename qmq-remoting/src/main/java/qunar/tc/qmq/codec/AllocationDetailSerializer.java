package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.PartitionAllocation;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class AllocationDetailSerializer extends ObjectSerializer<PartitionAllocation.AllocationDetail> {

    private ParameterizedType mapType = Types.newParamterizedType(null, Map.class, new Type[] {
       String.class,
       Types.newParamterizedType(null, Set.class, new Type[] {
               Integer.class
       })
    });
    private Serializer<Map> mapSerializer = getSerializer(mapType);

    @Override
    void doSerialize(PartitionAllocation.AllocationDetail allocationDetail, ByteBuf buf) {
        Map<String, Set<Integer>> clientId2PhysicalPartitions = allocationDetail.getClientId2PhysicalPartitions();
        mapSerializer.serialize(clientId2PhysicalPartitions, buf);
    }

    @Override
    PartitionAllocation.AllocationDetail doDeserialize(ByteBuf buf, Type type) {
        PartitionAllocation.AllocationDetail allocationDetail = new PartitionAllocation.AllocationDetail();
        allocationDetail.setClientId2PhysicalPartitions(mapSerializer.deserialize(buf, mapType));
        return allocationDetail;
    }
}
