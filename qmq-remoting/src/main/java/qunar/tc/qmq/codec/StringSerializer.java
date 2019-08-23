package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.utils.PayloadHolderUtils;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class StringSerializer implements Serializer<String> {

    @Override
    public void serialize(String s, ByteBuf buf) {
        PayloadHolderUtils.writeString(s, buf);
    }

    @Override
    public String deserialize(ByteBuf buf, Class... classes) {
        return PayloadHolderUtils.readString(buf);
    }
}
