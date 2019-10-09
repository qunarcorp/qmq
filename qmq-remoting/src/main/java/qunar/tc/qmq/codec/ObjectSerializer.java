package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.RemotingHeader;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public abstract class ObjectSerializer<T> implements Serializer<T> {

    private static final byte NULL = 0;
    private static final byte EXISTS = 1;

    @Override
    public void serialize(T t, ByteBuf buf, long version) {
        if (version >= RemotingHeader.getOrderedMessageVersion()) {
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

    abstract void doSerialize(T t, ByteBuf buf, long version);

    @Override
    public T deserialize(ByteBuf buf, Type type, long version) {
        byte exists = buf.readByte();
        if (version >= RemotingHeader.getOrderedMessageVersion()) {
            if (exists == NULL) {
                return null;
            } else {
                return doDeserialize(buf, type, version);
            }
        } else {
            return doDeserialize(buf, type, version);
        }
    }

    abstract T doDeserialize(ByteBuf buf, Type type, long version);
}
