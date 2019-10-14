package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class StringSerializer extends ObjectSerializer<String> {

    @Override
    void doSerialize(String s, ByteBuf buf, short version) {
        PayloadHolderUtils.writeString(s, buf);
    }

    @Override
    String doDeserialize(ByteBuf buf, Type type, short version) {
        return PayloadHolderUtils.readString(buf);
    }
}
