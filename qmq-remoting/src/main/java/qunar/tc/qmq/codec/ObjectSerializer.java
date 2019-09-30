package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public abstract class ObjectSerializer<T> implements Serializer<T> {

    private static final byte NULL = 0;
    private static final byte EXISTS = 1;

    @Override
    public void serialize(T t, ByteBuf buf) {
        if (t == null) {
            buf.writeByte(NULL);
        } else {
            buf.writeByte(EXISTS);
            doSerialize(t, buf);
        }
    }

    abstract void doSerialize(T t, ByteBuf buf);

    @Override
    public T deserialize(ByteBuf buf, Type type) {
        byte exists = buf.readByte();
        if (exists == NULL) {
            return null;
        } else {
            return doDeserialize(buf, type);
        }
    }

    abstract T doDeserialize(ByteBuf buf, Type type);
}
