package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class IntegerSerializer implements Serializer<Integer> {

    @Override
    public void serialize(Integer i, ByteBuf buf) {
        buf.writeInt(i);
    }

    @Override
    public Integer deserialize(ByteBuf buf, Class... classes) {
        return buf.readInt();
    }
}
