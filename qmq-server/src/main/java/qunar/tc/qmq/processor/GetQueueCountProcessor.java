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

package qunar.tc.qmq.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by zhaohui.yu
 * 6/19/18
 */
public class GetQueueCountProcessor extends AbstractRequestProcessor {
    private final MessageStoreWrapper store;

    public GetQueueCountProcessor(MessageStoreWrapper store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            List<Consumer> consumers = deserialize(request);
            List<Long> result = new ArrayList<>(consumers.size());
            for (Consumer consumer : consumers) {
                long queueCount = store.getQueueCount(consumer.subject, consumer.group);
                result.add(queueCount);
            }
            final Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, request.getHeader(), new GetQueueCountPayloadHolder(result));
            return CompletableFuture.completedFuture(response);
        } catch (Exception e) {
            final Datagram response = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.BROKER_ERROR, request.getHeader());
            return CompletableFuture.completedFuture(response);
        }
    }

    private List<Consumer> deserialize(RemotingCommand request) {
        ByteBuf body = request.getBody();
        List<Consumer> consumers = new ArrayList<>();
        while (body.readableBytes() > 0) {
            String subject = PayloadHolderUtils.readString(body);
            String group = PayloadHolderUtils.readString(body);
            consumers.add(new Consumer(subject, group));
        }
        return consumers;
    }

    private static class GetQueueCountPayloadHolder implements PayloadHolder {

        private final List<Long> result;

        public GetQueueCountPayloadHolder(List<Long> result) {
            this.result = result;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeShort((short) result.size());
            for (Long count : result) {
                out.writeLong(count);
            }
        }
    }

    private static class Consumer {
        public final String subject;

        public final String group;

        private Consumer(String subject, String group) {
            this.subject = subject;
            this.group = group;
        }
    }
}
