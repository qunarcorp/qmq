package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public interface Serializer<T> {

    void serialize(T t, ByteBuf buf);

    T deserialize(ByteBuf buf, Class... classes);
}
