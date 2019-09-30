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

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.*;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.processor.filters.Invoker;
import qunar.tc.qmq.processor.filters.ReceiveFilterChain;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.RetryPartitionUtils;
import qunar.tc.qmq.utils.SubjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author yunfeng.yang
 * @since 2017/8/7
 */
public class SendMessageWorker {
    private static final Logger LOG = LoggerFactory.getLogger(SendMessageWorker.class);

    private final DynamicConfig config;
    private final Invoker invoker;
    private final MessageStoreWrapper messageStore;
    private final Queue<ReceiveEntry> waitSlaveSyncQueue;

    public SendMessageWorker(final DynamicConfig config, final MessageStoreWrapper messageStore) {
        this.config = config;
        this.messageStore = messageStore;
        this.invoker = new ReceiveFilterChain().buildFilterChain(this::doInvoke);
        this.waitSlaveSyncQueue = new LinkedBlockingQueue<>();
    }

    ListenableFuture<Datagram> receive(final List<RawMessage> messages, final RemotingCommand cmd) {
        final List<SettableFuture<ReceiveResult>> futures = new ArrayList<>(messages.size());
        for (final RawMessage message : messages) {
            final MessageHeader header = message.getHeader();
            monitorMessageReceived(header.getCreateTime(), header.getSubject());

            final ReceivingMessage receivingMessage = new ReceivingMessage(message, cmd.getReceiveTime());
            futures.add(receivingMessage.promise());
            invoker.invoke(receivingMessage);
        }

        return Futures.transform(Futures.allAsList(futures),
                (Function<? super List<ReceiveResult>, ? extends Datagram>) input -> RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, cmd.getHeader(), new SendResultPayloadHolder(input)));
    }

    private void monitorMessageReceived(long receiveTime, String subject) {
        if (RetryPartitionUtils.isRetryPartitionName(subject)) {
            String[] subjectAndGroup = RetryPartitionUtils.parseSubjectAndGroup(subject);
            if (subjectAndGroup == null || subjectAndGroup.length != 2) return;
            QMon.consumerErrorCount(subjectAndGroup[0], subjectAndGroup[1]);
            return;
        }

        if (RetryPartitionUtils.isDeadRetryPartitionName(subject)) {
            String[] subjectAndGroup = RetryPartitionUtils.parseSubjectAndGroup(subject);
            if (subjectAndGroup == null || subjectAndGroup.length != 2) return;
            QMon.consumerErrorCount(subjectAndGroup[0], subjectAndGroup[1]);
            QMon.deadLetterQueueCount(subjectAndGroup[0], subjectAndGroup[1]);
            return;
        }

        QMon.receivedMessagesCountInc(subject);
        QMon.produceTime(subject, System.currentTimeMillis() - receiveTime);
    }

    private void doInvoke(ReceivingMessage message) {
        if (BrokerConfig.isReadonly()) {
            brokerReadOnly(message);
            return;
        }

        if (bigSlaveLag()) {
            brokerReadOnly(message);
            return;
        }

        final String subject = message.getSubject();
        if (SubjectUtils.isInValid(subject)) {
            QMon.receivedIllegalSubjectMessagesCountInc(subject);
            if (isRejectIllegalSubject()) {
                notAllowed(message);
                return;
            }
        }

        try {
            ReceiveResult result = messageStore.putMessage(message);
            offer(message, result);
        } catch (Throwable t) {
            error(message, t);
        }
    }

    private void notAllowed(ReceivingMessage message) {
        QMon.rejectReceivedMessageCountInc(message.getSubject());
        end(message, new ReceiveResult(message.getMessageId(), MessageProducerCode.SUBJECT_NOT_ASSIGNED, "message rejected", -1));
    }

    private boolean isRejectIllegalSubject() {
        return config.getBoolean("Receiver.RejectIllegalSubject", true);
    }

    private boolean bigSlaveLag() {
        return shouldWaitSlave() && waitSlaveSyncQueue.size() >= config.getInt("receive.queue.size", 50000);
    }

    private boolean shouldWaitSlave() {
        return config.getBoolean("wait.slave.wrote", false);
    }

    private void brokerReadOnly(ReceivingMessage message) {
        QMon.brokerReadOnlyMessageCountInc(message.getSubject());
        end(message, new ReceiveResult(message.getMessageId(), MessageProducerCode.BROKER_READ_ONLY, "BROKER READ ONLY", -1));
    }

    private void error(ReceivingMessage message, Throwable e) {
        LOG.error("save message error", e);
        QMon.receivedFailedCountInc(message.getSubject());
        end(message, new ReceiveResult(message.getMessageId(), MessageProducerCode.STORE_ERROR, "store error", -1));
    }

    private void offer(ReceivingMessage message, ReceiveResult result) {
        if (!message.isHigh()) {
            end(message, result);
            return;
        }
        if (result.getCode() != MessageProducerCode.SUCCESS) {
            end(message, result);
            return;
        }
        if (!shouldWaitSlave()) {
            end(message, result);
            return;
        }
        waitSlaveSyncQueue.offer(new ReceiveEntry(message, result));
    }

    private void end(ReceivingMessage message, ReceiveResult result) {
        try {
            message.done(result);
        } catch (Throwable e) {
            LOG.error("send response failed {}", message.getMessageId());
        } finally {
            QMon.processTime(message.getSubject(), System.currentTimeMillis() - message.getReceivedTime());
        }
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void syncRequest(SyncRequest syncRequest) {
        final long syncedOffset = syncRequest.getMessageLogOffset();
        ReceiveEntry first;
        while ((first = this.waitSlaveSyncQueue.peek()) != null) {
            if (first.result.getEndOffsetOfMessage() > syncedOffset) break;

            this.waitSlaveSyncQueue.poll();
            end(first.message, first.result);
        }
    }

    public static class SendResultPayloadHolder implements PayloadHolder {
        private final List<ReceiveResult> results;

        SendResultPayloadHolder(List<ReceiveResult> results) {
            this.results = results;
        }

        @Override
        public void writeBody(ByteBuf out) {
            for (ReceiveResult result : results) {
                int code = result.getCode();
                if (MessageProducerCode.SUCCESS == code) continue;

                writeItem(result, out);
            }
        }

        private void writeString(String str, ByteBuf out) {
            byte[] bytes = CharsetUtils.toUTF8Bytes(str);
            if (bytes != null) {
                out.writeShort((short) bytes.length);
                out.writeBytes(bytes);
            } else {
                out.writeShort(0);
            }
        }

        private void writeItem(ReceiveResult result, ByteBuf out) {
            int code = result.getCode();
            writeString(result.getMessageId(), out);
            out.writeInt(code);
            writeString(result.getRemark(), out);
        }
    }

    private static class ReceiveEntry {
        final ReceivingMessage message;
        final ReceiveResult result;

        ReceiveEntry(ReceivingMessage message, ReceiveResult result) {
            this.message = message;
            this.result = result;
        }
    }
}
