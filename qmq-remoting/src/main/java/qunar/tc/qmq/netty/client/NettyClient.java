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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.netty.DecodeHandler;
import qunar.tc.qmq.netty.EncodeHandler;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.util.RemoteHelper;

import java.util.concurrent.ExecutionException;

import static qunar.tc.qmq.netty.exception.ClientSendException.SendErrorCode;

/**
 * @author yiqun.fan create on 17-7-3.
 */
public class NettyClient extends AbstractNettyClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);
    private static final NettyClient INSTANCE = new NettyClient();

    public static NettyClient getClient() {
        return INSTANCE;
    }

    private NettyClientHandler clientHandler = new NettyClientHandler();

    private NettyClient() {
        super("qmq-client");
    }

    @Override
    protected void destroy() {
        clientHandler.shutdown();
    }

    @Override
    protected ChannelInitializer<SocketChannel> newChannelInitializer(final NettyClientConfig config, final DefaultEventExecutorGroup eventExecutors, final NettyConnectManageHandler connectManager) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(eventExecutors,
                        new EncodeHandler(),
                        new DecodeHandler(config.isServer()),
                        new IdleStateHandler(0, 0, config.getClientChannelMaxIdleTimeSeconds()),
                        connectManager,
                        clientHandler);
            }
        };
    }

    public Datagram sendSync(String brokerAddr, Datagram request, long responseTimeout) throws ClientSendException, InterruptedException, RemoteTimeoutException {
        ResultFuture result = new ResultFuture(brokerAddr);
        sendAsync(brokerAddr, request, responseTimeout, result);
        try {
            return result.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RemoteTimeoutException) {
                throw (RemoteTimeoutException) e.getCause();
            }

            if (e.getCause() instanceof ClientSendException) {
                throw (ClientSendException) e.getCause();
            }

            throw new RuntimeException(e.getCause());
        }
    }

    public ListenableFuture<Datagram> sendAsync(String brokerAddr, Datagram request, long responseTimeoutMills) throws ClientSendException {
        ResultFuture result = new ResultFuture(brokerAddr);
        sendAsync(brokerAddr, request, responseTimeoutMills, result);
        return result;
    }

    private static final class ResultFuture extends AbstractFuture<Datagram> implements ResponseFuture.Callback {
        private final String brokerAddr;

        ResultFuture(String brokerAddr) {
            this.brokerAddr = brokerAddr;
        }

        @Override
        public void processResponse(ResponseFuture responseFuture) {
            if (!responseFuture.isSendOk()) {
                setException(new ClientSendException(ClientSendException.SendErrorCode.WRITE_CHANNEL_FAIL));
                return;
            }

            if (responseFuture.isTimeout()) {
                setException(new RemoteTimeoutException(brokerAddr, responseFuture.getTimeout()));
                return;
            }

            Datagram response = responseFuture.getResponse();
            if (response != null) {
                set(response);
            } else {
                setException(new ClientSendException(SendErrorCode.BROKER_BUSY));
            }
        }
    }

    public void sendAsync(String brokerAddr, Datagram request, long responseTimeoutMills, ResponseFuture.Callback callback) throws ClientSendException {
        final Channel channel = getOrCreateChannel(brokerAddr);
        final ResponseFuture responseFuture = clientHandler.newResponse(channel, responseTimeoutMills, callback);
        request.getHeader().setOpaque(responseFuture.getOpaque());

        try {
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        responseFuture.setSendOk(true);
                        return;
                    }
                    clientHandler.removeResponse(channel, responseFuture);
                    responseFuture.completeBySendFail(future.cause());
                    LOGGER.error("send request to broker failed.", future.cause());
                    try {
                        responseFuture.executeCallbackOnlyOnce();
                    } catch (Throwable e) {
                        LOGGER.error("execute callback when send error exception", e);
                    }
                }
            });
        } catch (Exception e) {
            clientHandler.removeResponse(channel, responseFuture);
            responseFuture.completeBySendFail(e);
            LOGGER.warn("send request fail. brokerAddr={}", brokerAddr);
            throw new ClientSendException(SendErrorCode.WRITE_CHANNEL_FAIL, RemoteHelper.parseChannelRemoteAddress(channel), e);
        }
    }
}
