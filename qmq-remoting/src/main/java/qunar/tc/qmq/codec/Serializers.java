package qunar.tc.qmq.codec;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.QuerySubjectRequest;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ReleasePullLockRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;

import java.util.List;
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
        serializerMap.put(ProducerAllocation.class, new ProducerAllocationSerializer());
        serializerMap.put(PartitionProps.class, new PartitionPropsSerializer());
        serializerMap.put(RangeMap.class, new RangeMapSerializer());
        serializerMap.put(Range.class, new RangeSerializer());
        serializerMap.put(Map.class, new MapSerializer());
        serializerMap.put(Set.class, new SetSerializer());
        serializerMap.put(List.class, new ListSerializer());
        serializerMap.put(ConsumerAllocation.class, new ConsumerAllocationSerializer());
        serializerMap.put(MetaInfoResponse.class, new MetaInfoResponseSerializer());
        serializerMap.put(ProducerMetaInfoResponse.class, new ProducerMetaInfoResponseSerializer());
        serializerMap.put(ConsumerMetaInfoResponse.class, new ConsumerMetaInfoResponseSerializer());
        serializerMap.put(QuerySubjectRequest.class, new QuerySubjectRequestSerializer());
        serializerMap.put(ReleasePullLockRequest.class, new ReleasePullLockRequestSerializer());
    }

    public static <T> Serializer<T> getSerializer(Class<T> clazz) {
        return serializerMap.get(clazz);
    }
}
