package qunar.tc.qmq.codec;

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class RangeSerializer extends ObjectSerializer<Range> {

    @Override
    void doSerialize(Range range, ByteBuf buf) {
        Comparable lowerEndpoint = range.lowerEndpoint();
        Comparable upperEndpoint = range.upperEndpoint();
        Serializer serializer = getSerializer(lowerEndpoint.getClass());
        serializer.serialize(lowerEndpoint, buf);
        serializer.serialize(upperEndpoint, buf);
    }

    @Override
    Range doDeserialize(ByteBuf buf, Type type) {
        Type[] argTypes = ((ParameterizedType) type).getActualTypeArguments();
        Type rangeType = argTypes[0];
        Serializer rangeSerializer = getSerializer(rangeType);
        Comparable lowerBound = (Comparable) rangeSerializer.deserialize(buf, rangeType);
        Comparable upperBound = (Comparable) rangeSerializer.deserialize(buf, rangeType);
        return Range.closedOpen(lowerBound, upperBound);
    }
}
