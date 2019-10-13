package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.consumer.LockOperationRequest;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-18
 */
public class ReleasePullLockRequestSerializer extends ObjectSerializer<LockOperationRequest> {

    @Override
    void doSerialize(LockOperationRequest request, ByteBuf buf, short version) {
        PayloadHolderUtils.writeString(request.getPartitionName(), buf);
        PayloadHolderUtils.writeString(request.getConsumerGroup(), buf);
        PayloadHolderUtils.writeString(request.getClientId(), buf);
    }

    @Override
    LockOperationRequest doDeserialize(ByteBuf buf, Type type, short version) {
        String partitionName = PayloadHolderUtils.readString(buf);
        String consumerGroup = PayloadHolderUtils.readString(buf);
        String clientId = PayloadHolderUtils.readString(buf);
        return new LockOperationRequest(partitionName, consumerGroup, clientId);
    }
}
