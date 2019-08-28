package qunar.tc.qmq.codec;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class MapSerializer extends ObjectSerializer<Map> {

    @Override
    void doSerialize(Map map, ByteBuf buf) {
        int size = map.size();
        buf.writeInt(size);
        Set<Map.Entry> entrySet = map.entrySet();
        for (Map.Entry entry : entrySet) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            Serializer keySerializer = Serializers.getSerializer(key.getClass());
            Serializer valSerializer = Serializers.getSerializer(value.getClass());
            keySerializer.serialize(key, buf);
            valSerializer.serialize(value, buf);
        }
    }

    @Override
    Map doDeserialize(ByteBuf buf, Type type) {
        Type[] argTypes = ((ParameterizedType) type).getActualTypeArguments();
        Type keyType = argTypes[0];
        Type valType = argTypes[1];
        Serializer keySerializer = getSerializer(keyType);
        Serializer valSerializer = getSerializer(valType);
        HashMap<Object, Object> result = Maps.newHashMap();
        int size = buf.readInt();
        for (int i = 0; i < size; i++) {
            result.put(keySerializer.deserialize(buf, keyType), valSerializer.deserialize(buf, valType));
        }
        return result;
    }
}
