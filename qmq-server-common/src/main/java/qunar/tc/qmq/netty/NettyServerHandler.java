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

package qunar.tc.qmq.netty;

import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author yunfeng.yang
 * @since 2017/7/3
 */
@ChannelHandler.Sharable
public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServerHandler.class);

    private final Map<Short, NettyRequestExecutor> commands = Maps.newHashMap();

    void registerProcessor(short requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        this.commands.put(requestCode, new NettyRequestExecutor(requestCode, processor, executor));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand command) {
        command.setReceiveTime(System.currentTimeMillis());
        processMessageReceived(ctx, command);
    }

    private void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand cmd) {
        if (cmd != null) {
            switch (cmd.getCommandType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    private void processResponseCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
    }

    private void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final NettyRequestExecutor executor = commands.get(cmd.getHeader().getCode());
        if (executor == null) {
            cmd.release();
            LOG.error("unknown command code, code: {}", cmd.getHeader().getCode());
            Datagram response = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.UNKNOWN_CODE, cmd.getHeader());
            ctx.writeAndFlush(response);
        } else {
            executor.execute(ctx, cmd);
        }
    }
}
