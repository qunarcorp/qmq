package qunar.tc.qmq.codec;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * range map 序列化器, range 默认左闭右开
 *
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class RangeMapSerializer extends ObjectSerializer<RangeMap> {

    private Serializer<Range> rangeSerializer = Serializers.getSerializer(Range.class);

    @Override
    void doSerialize(RangeMap rangeMap, ByteBuf buf) {
        Map<Range<? extends Comparable>, Object> map = rangeMap.asMapOfRanges();
        buf.writeInt(map.size());

        for (Map.Entry<Range<? extends Comparable>, Object> entry : map.entrySet()) {
            Range<? extends Comparable> range = entry.getKey();
            Object value = entry.getValue();

            Serializer valSerializer = Serializers.getSerializer(value.getClass());

            rangeSerializer.serialize(range, buf);
            valSerializer.serialize(value, buf);
        }
    }

    @Override
    RangeMap doDeserialize(ByteBuf buf, Type type) {
        Type[] argTypes = ((ParameterizedType) type).getActualTypeArguments();
        Type keyType = argTypes[0];
        Type valType = argTypes[1];
        TreeRangeMap rangeMap = TreeRangeMap.create();
        int size = buf.readInt();
        Serializer valSerializer = getSerializer(valType);
        for (int i = 0; i < size; i++) {
            Range range = rangeSerializer.deserialize(buf, keyType);
            Object value = valSerializer.deserialize(buf, valType);
            rangeMap.put(range, value);
        }
        return rangeMap;
    }
}
