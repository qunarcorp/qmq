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

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yunfeng.yang
 * @since 2017/7/3
 */
class NettyRequestExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(NettyRequestExecutor.class);

    private final NettyRequestProcessor processor;
    private final ExecutorService executor;

    NettyRequestExecutor(final short requestCode, final NettyRequestProcessor processor, final ExecutorService executor) {
        this.processor = processor;
        this.executor = executor;
        if (executor != null) {
            if (executor instanceof ThreadPoolExecutor) {
                QMon.executorQueueSizeGauge(String.valueOf(requestCode), () -> (double) ((ThreadPoolExecutor) executor).getQueue().size());
            }
        }
    }

    void execute(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        if (executor == null) {
            executeWithMonitor(ctx, cmd);
            return;
        }

        try {
            executor.execute(() -> executeWithMonitor(ctx, cmd));
        } catch (RejectedExecutionException e) {
            ctx.writeAndFlush(errorResp(CommandCode.BROKER_ERROR, cmd));
        }
    }

    private void executeWithMonitor(final ChannelHandlerContext ctx, RemotingCommand cmd) {
        final long start = System.currentTimeMillis();
        try {
            doExecute(ctx, cmd);
        } finally {
            cmd.release();
            QMon.nettyRequestExecutorExecuteTimer(System.currentTimeMillis() - start);
        }
    }

    private void doExecute(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final int opaque = cmd.getHeader().getOpaque();

        if (processor.rejectRequest()) {
            ctx.writeAndFlush(errorResp(CommandCode.BROKER_REJECT, cmd));
            return;
        }

        try {
            final CompletableFuture<Datagram> future = processor.processRequest(ctx, cmd);
            if (cmd.isOneWay()) {
                return;
            }

            if (future != null) {
                future.exceptionally(ex -> errorResp(CommandCode.BROKER_ERROR, cmd))
                        .thenAccept((datagram -> {
                            final RemotingHeader header = datagram.getHeader();
                            header.setOpaque(opaque);
                            header.setVersion(cmd.getHeader().getVersion());
                            header.setRequestCode(cmd.getHeader().getCode());
                            ctx.writeAndFlush(datagram);
                        }));
            }
        } catch (Throwable e) {
            LOG.error("doExecute request exception, channel:{}, cmd:{}", ctx.channel(), cmd, e);
            ctx.writeAndFlush(errorResp(CommandCode.BROKER_ERROR, cmd));
        }
    }

    private Datagram errorResp(final short code, final RemotingCommand command) {
        return RemotingBuilder.buildEmptyResponseDatagram(code, command.getHeader());
    }
}
