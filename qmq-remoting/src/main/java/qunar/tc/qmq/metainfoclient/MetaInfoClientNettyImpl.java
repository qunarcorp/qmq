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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequestPayloadHolder;
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
    private ConcurrentSet<ResponseSubscriber> responseSubscribers = new ConcurrentSet<>();

    @Override
    public ListenableFuture<MetaInfoResponse> sendMetaInfoRequest(final MetaInfoRequest request) {
        try {
            SettableFuture<MetaInfoResponse> future = SettableFuture.create();
            MetaInfoResponseDecoder responseDecoder = new MetaInfoResponseDecoder(ClientType.of(request.getClientTypeCode()), future, responseSubscribers);
            sendRequest(
                    CommandCode.CLIENT_REGISTER,
                    META_INFO_RESPONSE_DECODER_NAME,
                    responseDecoder,
                    new MetaInfoRequestPayloadHolder(request)
            );
            return future;
        } catch (Exception e) {
            LOGGER.debug("request meta info exception. {}", request, e);
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public void registerResponseSubscriber(ResponseSubscriber subscriber) {
        responseSubscribers.add(subscriber);
    }

    private static final String QUERY_ORDERED_SUBJECT_DECODER_NAME = "queryOrderedSubjectDecoderName";

    @Override
    public void reportConsumerState(String subject, String consumerGroup, String clientId, OnOfflineState state) throws ClientSendException, MetaServerNotFoundException {
        sendRequest(
                CommandCode.REPORT_CONSUMER_ONLINE_STATE,
                out -> {
                    PayloadHolderUtils.writeString(subject, out);
                    PayloadHolderUtils.writeString(consumerGroup, out);
                    PayloadHolderUtils.writeString(clientId, out);
                    PayloadHolderUtils.writeString(state.name(), out);
                }
        );
    }

    @Override
    public boolean queryOrderedSubject(String subject) throws MetaServerNotFoundException, ClientSendException, ExecutionException, InterruptedException {
        SettableFuture<Boolean> future = SettableFuture.create();
        sendRequest(
                CommandCode.QUERY_ORDERED_SUBJECT,
                QUERY_ORDERED_SUBJECT_DECODER_NAME,
                new SimpleChannelInboundHandler<Datagram>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Datagram msg) throws Exception {
                        ByteBuf buf = msg.getBody();
                        future.set(buf.readBoolean());
                    }
                },
                out -> PayloadHolderUtils.writeString(subject, out)
        );
        return future.get();
    }
}
