package qunar.tc.qmq.metainfoclient;

import com.google.common.base.Optional;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.netty.DecodeHandler;
import qunar.tc.qmq.netty.EncodeHandler;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.AbstractNettyClient;
import qunar.tc.qmq.netty.client.NettyConnectManageHandler;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequestPayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class MetaServerNettyClient extends AbstractNettyClient implements MetaInfoClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaInfoClient.class);
    private static volatile MetaServerNettyClient instance;

    public static MetaServerNettyClient getClient(MetaServerLocator locator) {
        if (instance == null) {
            synchronized (MetaServerNettyClient.class) {
                if (instance == null) {
                    instance = new MetaServerNettyClient(locator);
                    if (!instance.isStarted()) {
                        NettyClientConfig config = new NettyClientConfig();
                        config.setClientWorkerThreads(1);
                        instance.start(config);
                    }
                }
            }
        }
        return instance;
    }

    private static final long UPDATE_INTERVAL = 1000 * 60;
    private ConcurrentLinkedQueue<MetaInfoClient.ResponseSubscriber> responseSubscribers = new ConcurrentLinkedQueue<>();
    private MetaServerCommandDecoder metaServerCommandDecoder = new MetaServerCommandDecoder();
    private MetaInfoResponseProcessor metaInfoResponseProcessor = new MetaInfoResponseProcessor(responseSubscribers);
    private QuerySubjectResponseProcessor querySubjectResponseProcessor = new QuerySubjectResponseProcessor();
    {
        metaServerCommandDecoder.registerProcessor(CommandCode.CLIENT_REGISTER, metaInfoResponseProcessor);
        metaServerCommandDecoder.registerProcessor(CommandCode.QUERY_SUBJECT, querySubjectResponseProcessor);
    }

    private volatile String metaServer;

    private volatile long lastUpdate;

    private MetaServerLocator locator;

    protected MetaServerNettyClient(MetaServerLocator locator) {
        super("qmq-metaclient");
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
                        connectManager,
                        metaServerCommandDecoder);
            }
        };
    }

    private String queryMetaServerAddress() {
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

    private ChannelFuture sendRequest(short commandCode, PayloadHolder payloadHolder) throws MetaServerNotFoundException, ClientSendException {
        String metaServer = queryMetaServerAddress();
        if (metaServer == null) {
            throw new MetaServerNotFoundException();
        }
        final Channel channel = getOrCreateChannel(metaServer);
        final Datagram datagram = RemotingBuilder.buildRequestDatagram(commandCode, payloadHolder);
        return channel.writeAndFlush(datagram);
    }

    private String queryMetaServerAddressWithRetry() {
        for (int i = 0; i < 3; ++i) {
            Optional<String> optional = locator.queryEndpoint();
            if (optional.isPresent())
                return optional.get();
        }
        return null;
    }

    @Override
    public ChannelFuture sendMetaInfoRequest(MetaInfoRequest request) {
        try {
            return sendRequest(CommandCode.CLIENT_REGISTER, new MetaInfoRequestPayloadHolder(request));
        } catch (Exception e) {
            LOGGER.warn("request meta info exception. {}", request, e);
            return null;
        }
    }

    @Override
    public void querySubject(QuerySubjectRequest request, QuerySubjectCallback callback) {
        try {
            querySubjectResponseProcessor.registerCallback(request, callback);
            sendRequest(
                    CommandCode.QUERY_SUBJECT,
                    out -> {
                        Serializer<QuerySubjectRequest> serializer = Serializers.getSerializer(QuerySubjectRequest.class);
                        serializer.serialize(request, out, RemotingHeader.getCurrentVersion());
                    }
            );
        } catch (Exception e) {
            LOGGER.debug("query history partition props error {}", request.getPartitionName(), e);
        }
    }

    @Override
    public void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber) {
        responseSubscribers.add(subscriber);
    }
}
