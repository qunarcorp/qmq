package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;

import static qunar.tc.qmq.protocol.RemotingHeader.supportOrderedMessage;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public abstract class ObjectSerializer<T> implements Serializer<T> {

    private static final byte NULL = 0;
    private static final byte EXISTS = 1;

    @Override
    public void serialize(T t, ByteBuf buf, short version) {
        if (supportOrderedMessage(version)) {
            if (t == null) {
                buf.writeByte(NULL);
            } else {
                buf.writeByte(EXISTS);
                doSerialize(t, buf, version);
            }
        } else {
            if (t == null) {
                throw new IllegalStateException("旧版本客户端无法处理 null 序列化, 必须有默认值");
            }
            doSerialize(t, buf, version);
        }
    }

    abstract void doSerialize(T t, ByteBuf buf, short version);

    @Override
    public T deserialize(ByteBuf buf, Type type, short version) {
        byte exists = buf.readByte();
        if (supportOrderedMessage(version)) {
            if (exists == NULL) {
                return null;
            } else {
                return doDeserialize(buf, type, version);
            }
        } else {
            return doDeserialize(buf, type, version);
        }
    }

    abstract T doDeserialize(ByteBuf buf, Type type, short version);
}
