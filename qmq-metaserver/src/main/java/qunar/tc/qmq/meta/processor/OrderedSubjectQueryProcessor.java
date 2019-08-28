package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.concurrent.CompletableFuture;

/**
 * 用于查询主题是否是顺序主题
 *
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class OrderedSubjectQueryProcessor implements NettyRequestProcessor {

    private CachedMetaInfoManager cachedMetaInfoManager;

    public OrderedSubjectQueryProcessor(CachedMetaInfoManager cachedMetaInfoManager) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        ByteBuf buf = request.getBody();
        String subject = PayloadHolderUtils.readString(buf);
        boolean isOrdered = cachedMetaInfoManager.getPartitionMapping(subject) != null;
        Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, request.getHeader(), out -> out.writeBoolean(isOrdered));
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
