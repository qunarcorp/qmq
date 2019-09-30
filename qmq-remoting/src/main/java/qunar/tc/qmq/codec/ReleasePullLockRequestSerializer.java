package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.consumer.ReleasePullLockRequest;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-18
 */
public class ReleasePullLockRequestSerializer extends ObjectSerializer<ReleasePullLockRequest> {

    @Override
    void doSerialize(ReleasePullLockRequest request, ByteBuf buf) {
        PayloadHolderUtils.writeString(request.getPartitionName(), buf);
        PayloadHolderUtils.writeString(request.getConsumerGroup(), buf);
        PayloadHolderUtils.writeString(request.getClientId(), buf);
    }

    @Override
    ReleasePullLockRequest doDeserialize(ByteBuf buf, Type type) {
        String partitionName = PayloadHolderUtils.readString(buf);
        String consumerGroup = PayloadHolderUtils.readString(buf);
        String clientId = PayloadHolderUtils.readString(buf);
        return new ReleasePullLockRequest(partitionName, consumerGroup, clientId);
    }
}
