package qunar.tc.qmq.codec;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;

import java.util.Map;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class Serializers {

    private static final Map<Class<?>, Serializer> serializerMap = Maps.newHashMap();

    static {
        serializerMap.put(Integer.class, new IntegerSerializer());
        serializerMap.put(String.class, new StringSerializer());
        serializerMap.put(Partition.class, new PartitionSerializer());
        serializerMap.put(PartitionAllocation.class, new PartitionAllocationSerializer());
        serializerMap.put(PartitionAllocation.AllocationDetail.class, new AllocationDetailSerializer());
        serializerMap.put(PartitionMapping.class, new PartitionMappingSerializer());
        serializerMap.put(RangeMap.class, new RangeMapSerializer());
        serializerMap.put(Range.class, new RangeSerializer());
        serializerMap.put(Map.class, new MapSerializer());
        serializerMap.put(Set.class, new SetSerializer());
    }

    public static <T> Serializer<T> getSerializer(Class<T> clazz) {
        return serializerMap.get(clazz);
    }
}
