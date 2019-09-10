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

package qunar.tc.qmq.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.config.NettyClientConfigManager;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.exception.ClientSendException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public abstract class AbstractNettyClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNettyClient.class);

    private final String clientName;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private EventLoopGroup eventLoopGroup;
    private DefaultEventExecutorGroup eventExecutors;
    private NettyConnectManageHandler connectManager;

    protected AbstractNettyClient(String clientName) {
        this.clientName = clientName;
    }

    public boolean isStarted() {
        return started.get();
    }

    public synchronized void start() {
        this.start(NettyClientConfigManager.get().getDefaultClientConfig());
    }

    public synchronized void start(NettyClientConfig config) {
        if (started.get()) {
            return;
        }
        Bootstrap bootstrap = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory(clientName + "-boss"));
        eventExecutors = new DefaultEventExecutorGroup(config.getClientWorkerThreads(), new DefaultThreadFactory(clientName + "-worker"));
        connectManager = new NettyConnectManageHandler(bootstrap, config.getConnectTimeoutMillis());
        bootstrap.group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMillis())
                .option(ChannelOption.SO_SNDBUF, config.getClientSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, config.getClientSocketRcvBufSize())
                .handler(newChannelInitializer(config, eventExecutors, connectManager));
        started.set(true);
    }

    public synchronized void shutdown() {
        if (!started.get()) {
            return;
        }
        try {
            connectManager.shutdown();
            eventLoopGroup.shutdownGracefully();
            eventExecutors.shutdownGracefully();
            destroy();
            started.set(false);
        } catch (Exception e) {
            LOGGER.error("NettyClient {} shutdown exception, ", clientName, e);
        }
    }

    protected void destroy() {
    }

    protected abstract ChannelInitializer<SocketChannel> newChannelInitializer(NettyClientConfig config, DefaultEventExecutorGroup eventExecutors, NettyConnectManageHandler connectManager);

    protected Channel getOrCreateChannel(String remoteAddr) throws ClientSendException {
        return connectManager.getOrCreateChannel(remoteAddr);
    }
}
