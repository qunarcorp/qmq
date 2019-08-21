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

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * @author yiqun.fan create on 17-8-31.
 */
@ChannelHandler.Sharable
class MetaInfoClientHandler extends SimpleChannelInboundHandler<Datagram> {

    private static final Logger LOG = LoggerFactory.getLogger(MetaInfoClientHandler.class);
    private static final MetaInfoResponseDeserializer deserializer = new AdaptiveMetaInfoResponseDeserializer();

    private final ConcurrentSet<MetaInfoClient.ResponseSubscriber> responseSubscribers = new ConcurrentSet<>();

    void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber) {
        responseSubscribers.add(subscriber);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Datagram msg) {
        MetaInfoResponse response = null;
        if (msg.getHeader().getCode() == CommandCode.SUCCESS) {
            response = deserializer.deserialize(msg.getHeader(), msg.getBody());
        }

        if (response != null) {
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

    private interface MetaInfoResponseDeserializer {

        short getVersion();

        MetaInfoResponse deserialize(RemotingHeader header, ByteBuf buf);
    }

    private static class AdaptiveMetaInfoResponseDeserializer implements MetaInfoResponseDeserializer {

        private TreeMap<Short, MetaInfoResponseDeserializer> deserializerMap;

        public AdaptiveMetaInfoResponseDeserializer() {
            this.deserializerMap = Maps.newTreeMap();
            MetaInfoResponseDeserializerV10 deserializerV10 = new MetaInfoResponseDeserializerV10();
            deserializerMap.put(deserializerV10.getVersion(), deserializerV10);
        }

        @Override
        public short getVersion() {
            return Short.MIN_VALUE;
        }

        @Override
        public MetaInfoResponse deserialize(RemotingHeader header, ByteBuf buf) {
            short version = header.getVersion();
            MetaInfoResponseDeserializer deserializer = deserializerMap.get(version);
            if (deserializer == null) {
                deserializer = deserializerMap.firstEntry().getValue();
            }
            return deserializer.deserialize(header, buf);
        }
    }

    private static class MetaInfoResponseDeserializerV10 implements MetaInfoResponseDeserializer {

        @Override
        public short getVersion() {
            return RemotingHeader.VERSION_10;
        }

        @Override
        public MetaInfoResponse deserialize(RemotingHeader header, ByteBuf buf) {
            try {
                final MetaInfoResponse metaInfoResponse = new MetaInfoResponse();
                metaInfoResponse.setTimestamp(buf.readLong());
                metaInfoResponse.setSubject(PayloadHolderUtils.readString(buf));
                metaInfoResponse.setConsumerGroup(PayloadHolderUtils.readString(buf));
                metaInfoResponse.setOnOfflineState(OnOfflineState.fromCode(buf.readByte()));
                metaInfoResponse.setClientTypeCode(buf.readByte());
                metaInfoResponse.setBrokerCluster(deserializeBrokerCluster(buf));
                // TODO 补充 partition 反序列化
                return metaInfoResponse;
            } catch (Exception e) {
                LOG.error("deserializeMetaInfoResponse exception", e);
            }
            return null;
        }
    }

    private static BrokerCluster deserializeBrokerCluster(ByteBuf buf) {
        final int brokerGroupSize = buf.readShort();
        final List<BrokerGroup> brokerGroups = new ArrayList<>(brokerGroupSize);
        for (int i = 0; i < brokerGroupSize; i++) {
            final BrokerGroup brokerGroup = new BrokerGroup();
            brokerGroup.setGroupName(PayloadHolderUtils.readString(buf));
            brokerGroup.setMaster(PayloadHolderUtils.readString(buf));
            brokerGroup.setUpdateTime(buf.readLong());
            final int brokerStateCode = buf.readByte();
            final BrokerState brokerState = BrokerState.codeOf(brokerStateCode);
            brokerGroup.setBrokerState(brokerState);
            brokerGroups.add(brokerGroup);
        }
        return new BrokerCluster(brokerGroups);
    }
}
