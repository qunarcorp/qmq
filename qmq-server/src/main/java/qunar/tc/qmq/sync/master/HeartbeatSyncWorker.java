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
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yunfeng.yang
 * @since 2017/8/20
 */
class HeartbeatSyncWorker implements SyncProcessor {
    private final Storage storage;
    private volatile long slaveMessageLogLag = 0;
    private volatile long slaveActionLogLag = 0;

    HeartbeatSyncWorker(Storage storage) {
        this.storage = storage;
        final String role = BrokerConfig.getBrokerRole().toString();
        QMon.slaveMessageLogLagGauge(role, () -> (double) slaveMessageLogLag);
        QMon.slaveActionLogLagGauge(role, () -> (double) slaveActionLogLag);
    }

    @Override
    public void process(SyncRequestEntry requestEntry) {
        final ChannelHandlerContext ctx = requestEntry.getCtx();
        final long messageLogMaxOffset = storage.getMaxMessageOffset();
        final long actionLogMaxOffset = storage.getMaxActionLogOffset();
        slaveMessageLogLag = messageLogMaxOffset - requestEntry.getSyncRequest().getMessageLogOffset();
        slaveActionLogLag = actionLogMaxOffset - requestEntry.getSyncRequest().getActionLogOffset();

        final HeartbeatPayloadHolder payloadHolder = new HeartbeatPayloadHolder(messageLogMaxOffset, actionLogMaxOffset);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, requestEntry.getRequestHeader(), payloadHolder);
        ctx.writeAndFlush(datagram);
    }

    @Override
    public void processTimeout(SyncRequestEntry entry) {
        // do nothing
    }

    private static class HeartbeatPayloadHolder implements PayloadHolder {
        private final long messageLogMaxOffset;
        private final long actionLogMaxOffset;

        HeartbeatPayloadHolder(long messageLogMaxOffset, long actionLogMaxOffset) {
            this.messageLogMaxOffset = messageLogMaxOffset;
            this.actionLogMaxOffset = actionLogMaxOffset;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeLong(messageLogMaxOffset);
            out.writeLong(actionLogMaxOffset);
        }
    }
}
