package qunar.tc.qmq.codec;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.netty.buffer.ByteBuf;

import java.util.Map;

/**
 * range map 序列化器, range 默认左闭右开
 *
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class RangeMapSerializer extends ObjectSerializer<RangeMap> {

    @Override
    void doSerialize(RangeMap rangeMap, ByteBuf buf) {
        Map<Range<? extends Comparable>, Object> map = rangeMap.asMapOfRanges();
        buf.writeInt(map.size());

        for (Map.Entry<Range<? extends Comparable>, Object> entry : map.entrySet()) {
            Range<? extends Comparable> range = entry.getKey();
            Object value = entry.getValue();
            Comparable lower = range.lowerEndpoint();
            Comparable upper = range.upperEndpoint();

            Serializer keySerializer = Serializers.getSerializer(lower.getClass());
            Serializer valSerializer = Serializers.getSerializer(value.getClass());

            keySerializer.serialize(lower, buf);
            keySerializer.serialize(upper, buf);
            valSerializer.serialize(value, buf);
        }
    }

    @Override
    RangeMap doDeserialize(ByteBuf buf, Class... classes) {
        Class keyClazz = classes[0];
        Class valClazz = classes[1];
        TreeRangeMap rangeMap = TreeRangeMap.create();
        int size = buf.readInt();
        Serializer keySerializer = Serializers.getSerializer(keyClazz);
        Serializer valSerializer = Serializers.getSerializer(valClazz);
        for (int i = 0; i < size; i++) {
            Comparable lower = (Comparable) keySerializer.deserialize(buf);
            Comparable upper = (Comparable) keySerializer.deserialize(buf);
            Range range = Range.closedOpen(lower, upper);
            Object value = valSerializer.deserialize(buf);
            rangeMap.put(range, value);
        }
        return rangeMap;
    }
}
