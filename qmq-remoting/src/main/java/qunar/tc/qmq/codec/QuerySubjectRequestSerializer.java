package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.QuerySubjectRequest;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-17
 */
public class QuerySubjectRequestSerializer extends ObjectSerializer<QuerySubjectRequest> {

    @Override
    void doSerialize(QuerySubjectRequest request, ByteBuf buf) {
        PayloadHolderUtils.writeString(request.getPartitionName(), buf);
    }

    @Override
    QuerySubjectRequest doDeserialize(ByteBuf buf, Type type) {
        String partitionName = PayloadHolderUtils.readString(buf);
        return new QuerySubjectRequest(partitionName);
    }
}
