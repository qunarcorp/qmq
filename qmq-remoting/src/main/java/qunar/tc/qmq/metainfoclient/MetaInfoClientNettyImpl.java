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

import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequestPayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.concurrent.ExecutionException;

/**
 * @author yiqun.fan create on 17-8-31.
 */
class MetaInfoClientNettyImpl extends MetaServerNettyClient implements MetaInfoClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaInfoClient.class);
    private static volatile MetaInfoClientNettyImpl instance;

    public static MetaInfoClientNettyImpl getClient(MetaServerLocator locator) {
        if (instance == null) {
            synchronized (MetaInfoClientNettyImpl.class) {
                if (instance == null) {
                    instance = new MetaInfoClientNettyImpl(locator);
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

    private MetaInfoClientNettyImpl(MetaServerLocator locator) {
        super("qmq-metaclient", locator);
    }

    private static final String META_INFO_RESPONSE_DECODER_NAME = "metaInfoResponseDecoder";
    private MetaInfoResponseDecoder metaInfoResponseDecoder = new MetaInfoResponseDecoder();

    @Override
    public void sendMetaInfoRequest(final MetaInfoRequest request) {
        try {
            String metaServer = queryMetaServerAddress();
            if (metaServer == null) return;
            final Channel channel = getOrCreateChannel(metaServer);
            final Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.CLIENT_REGISTER, new MetaInfoRequestPayloadHolder(request));
            addHandler(channel, META_INFO_RESPONSE_DECODER_NAME, metaInfoResponseDecoder);
            channel.writeAndFlush(datagram).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    LOGGER.debug("request meta info send success. {}", request);
                } else {
                    LOGGER.debug("request meta info send fail. {}", request);
                }
            });
        } catch (Exception e) {
            LOGGER.debug("request meta info exception. {}", request, e);
        }
    }

    @Override
    public void registerResponseSubscriber(ResponseSubscriber subscriber) {
        metaInfoResponseDecoder.registerResponseSubscriber(subscriber);
    }

    private static final String QUERY_ORDERED_SUBJECT_DECODER_NAME = "queryOrderedSubjectDecoderName";

    @Override
    public boolean queryOrderedSubject(String subject) throws MetaServerNotFoundException, ClientSendException, ExecutionException, InterruptedException {
        String metaServer = queryMetaServerAddress();
        if (metaServer == null) {
            throw new MetaServerNotFoundException();
        }
        final Channel channel = getOrCreateChannel(metaServer);
        SettableFuture<Boolean> future = SettableFuture.create();
        addHandler(channel, QUERY_ORDERED_SUBJECT_DECODER_NAME, new SimpleChannelInboundHandler<Datagram>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Datagram msg) throws Exception {
                ByteBuf buf = msg.getBody();
                future.set(buf.readBoolean());
            }
        });
        final Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.QUERY_ORDERED_SUBJECT, out -> PayloadHolderUtils.writeString(subject, out));
        channel.writeAndFlush(datagram);
        return future.get();
    }

    private void addHandler(Channel channel, String name, ChannelHandler handler) {
        ChannelPipeline pipeline = channel.pipeline();
        if (pipeline.get(name) == null) {
            pipeline.addLast(name, handler);
        }
    }
}
