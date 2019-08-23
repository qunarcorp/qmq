package qunar.tc.qmq.codec;

import com.google.common.collect.Maps;
import com.google.common.collect.RangeMap;
import qunar.tc.qmq.meta.PartitionInfo;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class Serializers {

    private static final Map<Class<?>, Serializer> serializerMap = Maps.newHashMap();

    static {
        serializerMap.put(Integer.class, new IntegerSerializer());
        serializerMap.put(String.class, new StringSerializer());
        serializerMap.put(PartitionInfo.class, new PartitionInfoSerializer());
        serializerMap.put(RangeMap.class, new RangeMapSerializer());
    }

    public static <T> Serializer<T> getSerializer(Class<T> clazz) {
        return serializerMap.get(clazz);
    }
}
