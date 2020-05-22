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

import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yunfeng.yang
 * @since 2017/7/27
 */
class AckMessageWorker implements ActorSystem.Processor<AckMessageProcessor.AckEntry> {
    private final ActorSystem actorSystem;
    private final ConsumerSequenceManager consumerSequenceManager;

    AckMessageWorker(final ActorSystem actorSystem, final ConsumerSequenceManager consumerSequenceManager) {
        this.actorSystem = actorSystem;
        this.consumerSequenceManager = consumerSequenceManager;
    }

    void ack(AckMessageProcessor.AckEntry entry) {
        actorSystem.dispatch("ack-" + entry.getGroup(), entry, this);
    }

    @Override
    public boolean process(final AckMessageProcessor.AckEntry ackEntry, ActorSystem.Actor<AckMessageProcessor.AckEntry> self) {
        Datagram response = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.SUCCESS, ackEntry.getRequestHeader());
        try {
            if (!consumerSequenceManager.putAckActions(ackEntry)) {
                response = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.BROKER_ERROR, ackEntry.getRequestHeader());
            }
        } catch (Exception e) {
            response = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.BROKER_ERROR, ackEntry.getRequestHeader());
        } finally {
            QMon.ackProcessTime(ackEntry.getSubject(), ackEntry.getGroup(), System.currentTimeMillis() - ackEntry.getAckBegin());
            ackEntry.getCtx().writeAndFlush(response);
        }
        return true;
    }
}
