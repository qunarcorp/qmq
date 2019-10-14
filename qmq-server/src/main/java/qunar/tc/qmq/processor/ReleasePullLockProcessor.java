package qunar.tc.qmq.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.order.ExclusiveConsumerLockManager;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.protocol.consumer.LockOperationRequest;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.concurrent.CompletableFuture;

/**
 * @author zhenwei.liu
 * @since 2019-09-18
 */
public class ReleasePullLockProcessor extends AbstractRequestProcessor {

    private final ExclusiveConsumerLockManager lockManager;

    public ReleasePullLockProcessor(ExclusiveConsumerLockManager lockManager) {
        this.lockManager = lockManager;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand command) {
        ByteBuf buf = command.getBody();
        Serializer<LockOperationRequest> serializer = Serializers.getSerializer(LockOperationRequest.class);
        LockOperationRequest request = serializer.deserialize(buf, null, RemotingHeader.getOrderedMessageVersion());
        boolean result = lockManager.releaseLock(request.getPartitionName(), request.getConsumerGroup(), request.getClientId());
        Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, command.getHeader(), out -> out.writeBoolean(result));
        return CompletableFuture.completedFuture(datagram);
    }
}
