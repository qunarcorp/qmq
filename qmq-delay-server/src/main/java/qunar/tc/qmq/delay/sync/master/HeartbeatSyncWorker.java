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

package qunar.tc.qmq.delay.sync.master;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.monitor.QMon;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.sync.SyncType;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-10 17:51
 */
public class HeartbeatSyncWorker implements DelaySyncRequestProcessor.SyncProcessor {
    private final DelayLogFacade delayLogFacade;

    private volatile long slaveMessageLogLag = 0;
    private volatile long slaveCurrentSegmentDispatchLogLag = 0;

    HeartbeatSyncWorker(DelayLogFacade delayLogFacade) {
        this.delayLogFacade = delayLogFacade;
        Metrics.gauge("slaveMessageLogLag", () -> (double) slaveMessageLogLag);
        Metrics.gauge("slaveCurrentSegmentDispatchLogLag", () -> (double) slaveCurrentSegmentDispatchLogLag);
    }

    @Override
    public void process(DelaySyncRequestProcessor.SyncRequestEntry entry) {
        final ChannelHandlerContext ctx = entry.getCtx();
        final long messageLogMaxOffset = delayLogFacade.getMessageLogMaxOffset();
        final long dispatchLogMaxOffset = delayLogFacade.getDispatchLogMaxOffset(entry.getDelaySyncRequest().getDispatchSegmentBaseOffset());

        slaveMessageLogLag = messageLogMaxOffset - entry.getDelaySyncRequest().getMessageLogOffset();
        slaveCurrentSegmentDispatchLogLag = dispatchLogMaxOffset - entry.getDelaySyncRequest().getDispatchLogOffset();
        QMon.slaveSyncLogOffset(SyncType.message.name(), slaveMessageLogLag);
        QMon.slaveSyncLogOffset(SyncType.dispatch.name(), slaveCurrentSegmentDispatchLogLag);
        final HeartBeatPayloadHolder heartBeatPayloadHolder = new HeartBeatPayloadHolder(messageLogMaxOffset, dispatchLogMaxOffset);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, entry.getRequestHeader(), heartBeatPayloadHolder);
        ctx.writeAndFlush(datagram);
    }

    @Override
    public void processTimeout(DelaySyncRequestProcessor.SyncRequestEntry entry) {

    }

    private static class HeartBeatPayloadHolder implements PayloadHolder {
        private final long messageLogMaxOffset;
        private final long dispatchLogOffset;

        HeartBeatPayloadHolder(long messageLogMaxOffset, long dispatchLogOffset) {
            this.messageLogMaxOffset = messageLogMaxOffset;
            this.dispatchLogOffset = dispatchLogOffset;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeLong(messageLogMaxOffset);
            out.writeLong(dispatchLogOffset);
        }
    }
}
