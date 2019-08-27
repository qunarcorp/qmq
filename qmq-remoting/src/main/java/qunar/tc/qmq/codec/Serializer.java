package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public interface Serializer<T> {

    void serialize(T t, ByteBuf buf);

    /**
     * 反序列化
     *
     * @param buf buf
     * @param type T 的类型
     * @return T
     */
    T deserialize(ByteBuf buf, Type type);

    default Serializer getSerializer(Type type) {
        Class clazz;
        if (type instanceof Class) {
            clazz = (Class) type;
        } else if (type instanceof ParameterizedType) {
            clazz = (Class) ((ParameterizedType) type).getRawType();
        } else {
            throw new IllegalArgumentException(String.format("不支持的类型 %s", type.getTypeName()));
        }
        return Serializers.getSerializer(clazz);
    }
}
