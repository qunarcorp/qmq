package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.meta.order.PartitionNameResolver;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.concurrent.CompletableFuture;

/**
 * @author zhenwei.liu
 * @since 2019-09-17
 */
public class QuerySubjectProcessor implements NettyRequestProcessor {

    private PartitionNameResolver partitionNameResolver;

    public QuerySubjectProcessor(PartitionNameResolver partitionNameResolver) {
        this.partitionNameResolver = partitionNameResolver;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand command) {
        ByteBuf buf = command.getBody();
        RemotingHeader header = command.getHeader();
        short version = header.getVersion();
        Serializer<QuerySubjectRequest> requestSerializer = Serializers.getSerializer(QuerySubjectRequest.class);
        QuerySubjectRequest request = requestSerializer.deserialize(buf, null, version);
        String partitionName = request.getPartitionName();

        Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, header, out -> {
            String subject = partitionNameResolver.getSubject(partitionName);
            QuerySubjectResponse response = null;
            if (subject != null) {
                response = new QuerySubjectResponse(subject, partitionName);
            }
            Serializer<QuerySubjectResponse> serializer = Serializers.getSerializer(QuerySubjectResponse.class);
            serializer.serialize(response, out, version);
        });

        return CompletableFuture.completedFuture(datagram);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
