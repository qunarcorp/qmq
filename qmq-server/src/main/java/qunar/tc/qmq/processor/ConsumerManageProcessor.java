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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumeManageRequest;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.concurrent.CompletableFuture;

/**
 * @author yunfeng.yang
 * @since 2017/11/22
 */
public class ConsumerManageProcessor extends AbstractRequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerManageProcessor.class);

    private final Storage store;

    public ConsumerManageProcessor(Storage store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand command) {
        final ConsumeManageRequest request = deserialize(command.getBody());
        LOG.info("receive consumer manager request:{}", request);

        if (!checkRequest(request)) {
            final Datagram datagram = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.PARAM_ERROR, command.getHeader());
            ctx.writeAndFlush(datagram);
            return null;
        }
        store.updateConsumeQueue(request.getSubject(), request.getGroup(), request.getConsumerFromWhere());

        final Datagram datagram = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.SUCCESS, command.getHeader());
        datagram.getHeader().setVersion(command.getHeader().getVersion());
        ctx.writeAndFlush(datagram);
        return null;
    }

    private boolean checkRequest(ConsumeManageRequest request) {
        return request != null && request.getSubject() != null && request.getGroup() != null;
    }

    private ConsumeManageRequest deserialize(ByteBuf buf) {
        String subject = PayloadHolderUtils.readString(buf);
        String consumerGroup = PayloadHolderUtils.readString(buf);
        int code = buf.readInt();
        ConsumeManageRequest request = new ConsumeManageRequest();
        request.setSubject(subject);
        request.setGroup(consumerGroup);
        request.setConsumerFromWhere(code);
        return request;
    }
}
