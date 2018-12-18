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

package qunar.tc.qmq.delay.receiver;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.delay.meta.BrokerRoleManager;
import qunar.tc.qmq.delay.store.model.RawMessageExtend;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.utils.Flags;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static qunar.tc.qmq.protocol.QMQSerializer.deserializeMessageHeader;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-27 17:36
 */
public class ReceivedDelayMessageProcessor implements NettyRequestProcessor {
    private final Receiver receiver;

    public ReceivedDelayMessageProcessor(Receiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        final List<RawMessageExtend> messages = deserializeRawMessagesExtend(request);
        final ListenableFuture<Datagram> result = receiver.receive(messages, request);
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
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public boolean rejectRequest() {
        return !BrokerRoleManager.isDelayMaster();
    }

    private List<RawMessageExtend> deserializeRawMessagesExtend(RemotingCommand request) {
        final ByteBuf body = request.getBody();
        if (body.readableBytes() == 0) return Collections.emptyList();

        List<RawMessageExtend> messages = Lists.newArrayList();
        while (body.isReadable()) {
            messages.add(doDeserializeRawMessagesExtend(body));
        }
        return messages;
    }

    private RawMessageExtend doDeserializeRawMessagesExtend(ByteBuf body) {
        body.markReaderIndex();
        int headerStart = body.readerIndex();
        long bodyCrc = body.readLong();
        MessageHeader header = deserializeMessageHeader(body);
        header.setBodyCrc(bodyCrc);
        int bodyLen = body.readInt();
        int headerLen = body.readerIndex() - headerStart;
        int totalLen = headerLen + bodyLen;

        body.resetReaderIndex();
        ByteBuf messageBuf = body.readSlice(totalLen);
        // client config error,prefer to send after ten second
        long scheduleTime = System.currentTimeMillis() + 10000;
        if (Flags.isDelay(header.getFlag())) {
            scheduleTime = header.getExpireTime();
        }

        return new RawMessageExtend(header, messageBuf, messageBuf.readableBytes(), scheduleTime);
    }
}
