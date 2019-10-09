package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class IntegerSerializer implements Serializer<Integer> {

    @Override
    public void serialize(Integer i, ByteBuf buf, long version) {
        buf.writeInt(i);
    }

    @Override
    public Integer deserialize(ByteBuf buf, Type type, long version) {
        return buf.readInt();
    }
}
