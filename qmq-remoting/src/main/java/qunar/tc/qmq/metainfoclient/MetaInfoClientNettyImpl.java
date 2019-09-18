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
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.QuerySubjectRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequestPayloadHolder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

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
    public ListenableFuture<String> querySubject(QuerySubjectRequest request) {
        try {
            SettableFuture<String> future = SettableFuture.create();
            sendRequest(
                    CommandCode.QUERY_SUBJECT,
                    "QUERY_SUBJECT",
                    new SimpleChannelInboundHandler<Datagram>() {
                        @Override
                        @SuppressWarnings("unchecked")
                        protected void channelRead0(ChannelHandlerContext ctx, Datagram msg) throws Exception {
                            ByteBuf buf = msg.getBody();
                            future.set(PayloadHolderUtils.readString(buf));
                        }
                    },
                    out -> {
                        Serializer<QuerySubjectRequest> serializer = Serializers.getSerializer(QuerySubjectRequest.class);
                        serializer.serialize(request, out);
                    }
            );
            return future;
        } catch (Exception e) {
            LOGGER.debug("query history partition props error {}", request.getPartitionName(), e);
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public void registerResponseSubscriber(ResponseSubscriber subscriber) {
        responseSubscribers.add(subscriber);
    }
}
