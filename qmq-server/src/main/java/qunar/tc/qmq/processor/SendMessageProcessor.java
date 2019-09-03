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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.stats.BrokerStats;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static qunar.tc.qmq.protocol.QMQSerializer.deserializeMessageHeader;

/**
 * @author yunfeng.yang
 * @since 2017/7/4
 */
public class SendMessageProcessor extends AbstractRequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SendMessageProcessor.class);

    private final SendMessageWorker sendMessageWorker;

    public SendMessageProcessor(SendMessageWorker sendMessageWorker) {
        this.sendMessageWorker = sendMessageWorker;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand command) {
        // TODO(zhenwei.liu) 这里需要处理顺序连续失败问题
        List<RawMessage> messages;
        try {
            messages = deserializeRawMessages(command);
        } catch (Exception e) {
            LOG.error("received invalid message. channel: {}", ctx.channel(), e);
            QMon.brokerReceivedInvalidMessageCountInc();

            final Datagram response = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.BROKER_ERROR, command.getHeader());
            return CompletableFuture.completedFuture(response);
        }

        BrokerStats.getInstance().getLastMinuteSendRequestCount().add(messages.size());

        final ListenableFuture<Datagram> result = sendMessageWorker.receive(messages, command);
        final CompletableFuture<Datagram> future = new CompletableFuture<>();
        Futures.addCallback(result, new FutureCallback<Datagram>() {
                    @Override
                    public void onSuccess(Datagram datagram) {
                        future.complete(datagram);
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        future.completeExceptionally(ex);
                    }
                }
        );
        return future;
    }

    private List<RawMessage> deserializeRawMessages(final RemotingCommand command) {
        final ByteBuf body = command.getBody();
        if (body.readableBytes() == 0) return Collections.emptyList();

        List<RawMessage> messages = Lists.newArrayList();
        while (body.isReadable()) {
            messages.add(deserializeRawMessageWithCrc(body));
        }
        return messages;
    }

    private RawMessage deserializeRawMessageWithCrc(ByteBuf body) {
        long bodyCrc = body.readLong();

        int headerStart = body.readerIndex();
        body.markReaderIndex();
        MessageHeader header = deserializeMessageHeader(body);
        header.setBodyCrc(bodyCrc);
        int bodyLen = body.readInt();
        int headerLen = body.readerIndex() - headerStart;

        int totalLen = headerLen + bodyLen;
        body.resetReaderIndex();
        ByteBuf messageBuf = body.readSlice(totalLen);
        return new RawMessage(header, messageBuf, totalLen);
    }

}
