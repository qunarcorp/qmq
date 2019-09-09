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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;

/**
 * @author yiqun.fan create on 17-8-31.
 */
class MetaInfoResponseDecoder extends SimpleChannelInboundHandler<Datagram> {

    private static final Logger LOG = LoggerFactory.getLogger(MetaInfoResponseDecoder.class);

    private final ConcurrentSet<MetaInfoClient.ResponseSubscriber> responseSubscribers;
    private final SettableFuture<MetaInfoResponse> future;
    private final ClientType clientType;

    public MetaInfoResponseDecoder(ClientType clientType, SettableFuture<MetaInfoResponse> future, ConcurrentSet<MetaInfoClient.ResponseSubscriber> responseSubscribers) {
        this.clientType = clientType;
        this.future = future;
        this.responseSubscribers = responseSubscribers;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Datagram msg) {
        MetaInfoResponse response = null;
        if (msg.getHeader().getCode() == CommandCode.SUCCESS) {
            if (clientType.isConsumer()) {
                Serializer<ConsumerMetaInfoResponse> serializer = Serializers.getSerializer(ConsumerMetaInfoResponse.class);
                response = serializer.deserialize(msg.getBody(), null);
            } else {
                Serializer<ProducerMetaInfoResponse> serializer = Serializers.getSerializer(ProducerMetaInfoResponse.class);
                response = serializer.deserialize(msg.getBody(), null);
            }
        }

        if (response != null) {
            future.set(response);
            notifySubscriber(response);
        } else {
            LOG.warn("request meta info UNKNOWN. code={}", msg.getHeader().getCode());
        }
    }

    private void notifySubscriber(MetaInfoResponse response) {
        for (MetaInfoClient.ResponseSubscriber subscriber : responseSubscribers) {
            try {
                subscriber.onResponse(response);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

}
