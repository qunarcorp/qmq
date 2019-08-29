package qunar.tc.qmq.metainfoclient;

import com.google.common.base.Optional;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.netty.DecodeHandler;
import qunar.tc.qmq.netty.EncodeHandler;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.AbstractNettyClient;
import qunar.tc.qmq.netty.client.NettyConnectManageHandler;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public abstract class MetaServerNettyClient extends AbstractNettyClient {

    private volatile String metaServer;

    private volatile long lastUpdate;

    private static final long UPDATE_INTERVAL = 1000 * 60;

    private MetaServerLocator locator;

    protected MetaServerNettyClient(String clientName, MetaServerLocator locator) {
        super(clientName);
        this.locator = locator;
    }

    @Override
    protected ChannelInitializer<SocketChannel> newChannelInitializer(NettyClientConfig config, DefaultEventExecutorGroup eventExecutors, NettyConnectManageHandler connectManager) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(eventExecutors,
                        new EncodeHandler(),
                        new DecodeHandler(false),
                        new IdleStateHandler(0, 0, config.getClientChannelMaxIdleTimeSeconds()),
                        connectManager);
            }
        };
    }

    protected String queryMetaServerAddress() {
        if (metaServer == null) {
            metaServer = queryMetaServerAddressWithRetry();
            lastUpdate = System.currentTimeMillis();
            return metaServer;
        }
        if (System.currentTimeMillis() - lastUpdate > UPDATE_INTERVAL) {
            Optional<String> optional = locator.queryEndpoint();
            if (optional.isPresent()) {
                this.metaServer = optional.get();
                lastUpdate = System.currentTimeMillis();
            }
        }
        return metaServer;
    }

    protected void sendRequest(short commandCode, PayloadHolder payloadHolder) throws MetaServerNotFoundException, ClientSendException {
        sendRequest(commandCode, null, null, payloadHolder);
    }

    protected void sendRequest(short commandCode, String decodeHandlerName, ChannelHandler decodeHandler, PayloadHolder payloadHolder) throws MetaServerNotFoundException, ClientSendException {
        String metaServer = queryMetaServerAddress();
        if (metaServer == null) {
            throw new MetaServerNotFoundException();
        }
        final Channel channel = getOrCreateChannel(metaServer);
        if (decodeHandlerName != null && decodeHandler != null) {
            addHandler(channel, decodeHandlerName, decodeHandler);
        }
        final Datagram datagram = RemotingBuilder.buildRequestDatagram(commandCode, payloadHolder);
        channel.writeAndFlush(datagram);
    }

    protected void addHandler(Channel channel, String name, ChannelHandler handler) {
        ChannelPipeline pipeline = channel.pipeline();
        if (pipeline.get(name) == null) {
            pipeline.addLast(name, handler);
        }
    }

    private String queryMetaServerAddressWithRetry() {
        for (int i = 0; i < 3; ++i) {
            Optional<String> optional = locator.queryEndpoint();
            if (optional.isPresent())
                return optional.get();
        }
        return null;
    }
}
