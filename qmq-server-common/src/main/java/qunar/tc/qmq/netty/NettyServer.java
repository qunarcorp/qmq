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

package qunar.tc.qmq.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;

import java.util.concurrent.ExecutorService;

/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public class NettyServer implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;

    private final ServerBootstrap bootstrap;
    private final NettyServerHandler serverHandler;
    private final ConnectionHandler connectionHandler;

    private final int port;
    private volatile Channel channel;

    public NettyServer(final String name, final int workerCount, final int port, final ConnectionEventHandler connectionEventHandler) {
        this.bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory(name + "-netty-server-boss", true));
        this.workerGroup = new NioEventLoopGroup(workerCount, new DefaultThreadFactory(name + "-netty-server-worker", true));
        this.port = port;
        this.bootstrap = new ServerBootstrap();
        this.serverHandler = new NettyServerHandler();
        this.connectionHandler = new ConnectionHandler(connectionEventHandler);
    }

    public void registerProcessor(final short requestCode, final NettyRequestProcessor processor) {
        registerProcessor(requestCode, processor, null);
    }

    public void registerProcessor(final short requestCode,
                                  final NettyRequestProcessor processor,
                                  final ExecutorService executorService) {
        serverHandler.registerProcessor(requestCode, processor, executorService);
    }

    public void start() {
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("connectionHandler", connectionHandler);
                        ch.pipeline().addLast("encoder", new EncodeHandler());
                        ch.pipeline().addLast("decoder", new DecodeHandler(true));
                        ch.pipeline().addLast("dispatcher", serverHandler);
                    }
                });
        try {
            channel = bootstrap.bind(port).await().channel();
        } catch (InterruptedException e) {
            LOG.error("server start fail", e);
        }
        LOG.info("listen on port {}", port);
    }

    @Override
    public void destroy() {
        if (channel != null && channel.isActive()) {
            channel.close().awaitUninterruptibly();
        }
    }
}
