package qunar.tc.qmq.codec;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class ListSerializer extends ObjectSerializer<List> {

    @Override
    void doSerialize(List list, ByteBuf buf) {
        buf.writeInt(list.size());
        for (Object o : list) {
            Serializer serializer = getSerializer(o.getClass());
            serializer.serialize(o, buf);
        }
    }

    @Override
    List doDeserialize(ByteBuf buf, Type type) {
        int size = buf.readInt();
        ArrayList<Object> list = Lists.newArrayListWithCapacity(size);
        Type elementType = ((ParameterizedType) type).getActualTypeArguments()[0];
        Serializer elementSerializer = getSerializer(elementType);
        for (int i = 0; i < size; i++) {
            list.add(elementSerializer.deserialize(buf, type));
        }
        return list;
    }
}
