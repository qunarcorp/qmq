/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.delay.receiver;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.delay.meta.BrokerRoleManager;
import qunar.tc.qmq.delay.store.model.RawMessageExtend;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.Crc32;
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

    public static List<RawMessageExtend> deserializeRawMessagesExtend(RemotingCommand request) {
        final ByteBuf body = request.getBody();
        if (body.readableBytes() == 0) return Collections.emptyList();

        List<RawMessageExtend> messages = Lists.newArrayList();
        short version = request.getHeader().getVersion();
        while (body.isReadable()) {
            if (version >= RemotingHeader.VERSION_7) {
                messages.add(doDeserializeRawMessagesExtend(body));
            } else if (version == RemotingHeader.VERSION_6) {
                messages.add(deserializeRawMessageExtendWithCrc(body));
            } else if (version == RemotingHeader.VERSION_5) {
                messages.add(deserializeRawMessageExtendIgnoreCrc(body));
            } else {
                messages.add(deserializeRawMessageExtend(body));
            }
        }
        return messages;
    }

    private static RawMessageExtend doDeserializeRawMessagesExtend(ByteBuf body) {
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

    private static RawMessageExtend deserializeRawMessageExtendWithCrc(ByteBuf body) {
        body.markReaderIndex();
        int headerStart = body.readerIndex();
        long bodyCrc = body.readLong();
        MessageHeader header = deserializeMessageHeader(body);
        header.setBodyCrc(bodyCrc);
        int bodyLen = body.readInt();
        int headerLen = body.readerIndex() - headerStart;
        int totalLen = headerLen + bodyLen;
        long scheduleTime = getScheduleTime(bodyLen, body);

        body.resetReaderIndex();
        ByteBuf messageBuf = body.readSlice(totalLen);
        return new RawMessageExtend(header, messageBuf, messageBuf.readableBytes(), scheduleTime);
    }

    private static long getScheduleTime(int bodyLen, ByteBuf body) {
        ByteBuf buffer = body.readSlice(bodyLen);
        while (buffer.isReadable(Short.BYTES)) {
            short keySize = buffer.readShort();
            if (!buffer.isReadable(keySize)) return -1;
            byte[] keyBs = new byte[keySize];
            buffer.readBytes(keyBs);

            if (!buffer.isReadable(Short.BYTES)) return -1;
            int valSize = buffer.readShort();
            if (!buffer.isReadable(valSize)) return -1;

            String key = CharsetUtils.toUTF8String(keyBs);
            if (BaseMessage.keys.qmq_scheduleReceiveTime.name().equalsIgnoreCase(key)) {
                byte[] valBs = new byte[valSize];
                buffer.readBytes(valBs);
                return Long.valueOf(CharsetUtils.toUTF8String(valBs));
            } else {
                buffer.skipBytes(valSize);
            }
        }

        return -1;
    }

    private static RawMessageExtend deserializeRawMessageExtendIgnoreCrc(ByteBuf body) {
        //skip wrong crc
        body.readLong();

        body.markReaderIndex();
        int headerStart = body.readerIndex();

        MessageHeader header = deserializeMessageHeader(body);
        int bodyLen = body.readInt();
        int headerLen = body.readerIndex() - headerStart;
        int totalLen = headerLen + bodyLen;

        long scheduleTime = getScheduleTime(bodyLen, body);

        body.resetReaderIndex();
        byte[] data = new byte[totalLen];
        body.readBytes(data);
        long crc = Crc32.crc32(data);
        header.setBodyCrc(crc);

        ByteBuf buffer = Unpooled.buffer(8 + totalLen);
        buffer.writeLong(crc);
        buffer.writeBytes(data);

        return new RawMessageExtend(header, buffer, buffer.readableBytes(), scheduleTime);
    }

    private static RawMessageExtend deserializeRawMessageExtend(ByteBuf body) {
        body.markReaderIndex();
        int headerStart = body.readerIndex();
        MessageHeader header = deserializeMessageHeader(body);

        int bodyLen = body.readInt();
        int headerLen = body.readerIndex() - headerStart;
        int totalLen = headerLen + bodyLen;

        long scheduleTime = getScheduleTime(bodyLen, body);

        body.resetReaderIndex();
        byte[] data = new byte[totalLen];
        body.readBytes(data);
        long crc = Crc32.crc32(data);
        header.setBodyCrc(crc);

        ByteBuf buffer = Unpooled.buffer(8 + totalLen);
        buffer.writeLong(crc);
        buffer.writeBytes(data);

        return new RawMessageExtend(header, buffer, buffer.readableBytes(), scheduleTime);
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
}
