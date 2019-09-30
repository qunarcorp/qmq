package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.QuerySubjectResponse;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-24
 */
public class QuerySubjectResponseSerializer extends ObjectSerializer<QuerySubjectResponse> {

    @Override
    void doSerialize(QuerySubjectResponse response, ByteBuf buf) {
        PayloadHolderUtils.writeString(response.getSubject(), buf);
        PayloadHolderUtils.writeString(response.getPartitionName(), buf);
    }

    @Override
    QuerySubjectResponse doDeserialize(ByteBuf buf, Type type) {
        String subject = PayloadHolderUtils.readString(buf);
        String partitionName = PayloadHolderUtils.readString(buf);
        return new QuerySubjectResponse(subject, partitionName);
    }
}
