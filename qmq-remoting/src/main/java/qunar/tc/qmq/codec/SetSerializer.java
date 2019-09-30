package qunar.tc.qmq.codec;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class SetSerializer extends ObjectSerializer<Set> {

    @Override
    void doSerialize(Set set, ByteBuf buf) {
        int size = set.size();
        buf.writeInt(size);
        for (Object element : set) {
            Serializer serializer = Serializers.getSerializer(element.getClass());
            serializer.serialize(element, buf);
        }
    }

    @Override
    Set doDeserialize(ByteBuf buf, Type type) {
        Type[] argTypes = ((ParameterizedType) type).getActualTypeArguments();
        Type argType = argTypes[0];
        Serializer serializer = getSerializer(argType);
        HashSet<Object> set = Sets.newHashSet();
        int size = buf.readInt();
        for (int i = 0; i < size; i++) {
            set.add(serializer.deserialize(buf, argType));
        }
        return set;
    }
}
