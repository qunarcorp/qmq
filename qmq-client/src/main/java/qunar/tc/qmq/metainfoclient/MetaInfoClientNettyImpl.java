/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.metainfoclient;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.netty.DecodeHandler;
import qunar.tc.qmq.netty.EncodeHandler;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.AbstractNettyClient;
import qunar.tc.qmq.netty.client.NettyConnectManageHandler;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequestPayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yiqun.fan create on 17-8-31.
 */
class MetaInfoClientNettyImpl extends AbstractNettyClient implements MetaInfoClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaInfoClient.class);

    private MetaServerLocator locator;

    public static MetaInfoClientNettyImpl getClient() {
        MetaInfoClientNettyImpl client = SUPPLIER.get();
        if (!client.isStarted()) {
            NettyClientConfig config = new NettyClientConfig();
            config.setClientWorkerThreads(1);
            client.start(config);
        }
        return client;
    }

    private static final Supplier<MetaInfoClientNettyImpl> SUPPLIER = Suppliers.memoize(new Supplier<MetaInfoClientNettyImpl>() {
        @Override
        public MetaInfoClientNettyImpl get() {
            return new MetaInfoClientNettyImpl();
        }
    });

    private MetaInfoClientNettyImpl() {
        super("qmq-metaclient");
    }

    private MetaInfoClientHandler clientHandler;

    @Override
    protected void initHandler() {
        clientHandler = new MetaInfoClientHandler();
    }

    @Override
    protected ChannelInitializer<SocketChannel> newChannelInitializer(final NettyClientConfig config, final DefaultEventExecutorGroup eventExecutors, final NettyConnectManageHandler connectManager) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(eventExecutors,
                        new EncodeHandler(),
                        new DecodeHandler(false),
                        new IdleStateHandler(0, 0, config.getClientChannelMaxIdleTimeSeconds()),
                        connectManager,
                        clientHandler);
            }
        };
    }

    @Override
    public void sendRequest(final MetaInfoRequest request) {
        try {
            String metaServer = queryMetaServerAddress();
            if (metaServer == null) return;
            final Channel channel = getOrCreateChannel(metaServer);
            final Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.CLIENT_REGISTER, new MetaInfoRequestPayloadHolder(request));
            channel.writeAndFlush(datagram).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        LOGGER.debug("MetaInfoClientNettyImpl", "request meta info send success. {}", request);
                    } else {
                        LOGGER.debug("MetaInfoClientNettyImpl", "request meta info send fail. {}", request);
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.debug("MetaInfoClientNettyImpl", "request meta info exception. {}", request, e);
        }
    }

    private volatile String metaServer;

    private volatile long lastUpdate;

    private static final long UPDATE_INTERVAL = 1000 * 60;

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

    private String queryMetaServerAddressWithRetry() {
        for (int i = 0; i < 3; ++i) {
            Optional<String> optional = locator.queryEndpoint();
            if (optional.isPresent())
                return optional.get();
        }
        return null;
    }

    @Override
    public void registerResponseSubscriber(ResponseSubscriber subscriber) {
        clientHandler.registerResponseSubscriber(subscriber);
    }

    public void setMetaServerLocator(MetaServerLocator locator) {
        this.locator = locator;
    }
}
