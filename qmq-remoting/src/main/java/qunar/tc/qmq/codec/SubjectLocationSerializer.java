package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.SubjectLocation;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class SubjectLocationSerializer extends ObjectSerializer<SubjectLocation> {

    @Override
    void doSerialize(SubjectLocation subjectLocation, ByteBuf buf) {
        PayloadHolderUtils.writeString(subjectLocation.getSubjectSuffix(), buf);
        PayloadHolderUtils.writeString(subjectLocation.getBrokerGroup(), buf);
    }

    @Override
    SubjectLocation doDeserialize(ByteBuf buf, Type type) {
        String subjectSuffix = PayloadHolderUtils.readString(buf);
        String brokerGroup = PayloadHolderUtils.readString(buf);
        return new SubjectLocation(subjectSuffix, brokerGroup);
    }
}
