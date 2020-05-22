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

package qunar.tc.qmq.sync.master;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.store.CheckpointManager;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.concurrent.CompletableFuture;

/**
 * @author keli.wang
 * @since 2018/10/29
 */
class SyncCheckpointProcessor implements NettyRequestProcessor {
    private final CheckpointManager checkpointManager;

    SyncCheckpointProcessor(Storage storage) {
        this.checkpointManager = storage.getCheckpointManager();
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        final byte[] message = checkpointManager.dumpMessageCheckpoint();
        final byte[] action = checkpointManager.dumpActionCheckpoint();
        final CheckpointPayloadHolder payloadHolder = new CheckpointPayloadHolder(message, action);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, request.getHeader(), payloadHolder);
        return CompletableFuture.completedFuture(datagram);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private static class CheckpointPayloadHolder implements PayloadHolder {
        private static final int V1 = 1;

        private final byte[] message;
        private final byte[] action;

        CheckpointPayloadHolder(final byte[] message, final byte[] action) {
            this.message = message;
            this.action = action;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeByte(V1);
            out.writeInt(message.length);
            out.writeBytes(message);
            out.writeInt(action.length);
            out.writeBytes(action);
        }
    }
}
