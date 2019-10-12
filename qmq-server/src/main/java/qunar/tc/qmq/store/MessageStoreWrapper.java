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

package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.*;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.order.ExclusiveConsumerLockManager;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;

/**
 * @author yunfeng.yang
 * @since 2017/7/10
 */
public class MessageStoreWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageStoreWrapper.class);

    private final Storage storage;

    private final SharedMessageReader sharedMessageReader;
    private final ExclusiveMessageReader exclusiveMessageReader;

    public MessageStoreWrapper(final DynamicConfig config, final Storage storage, final ConsumerSequenceManager consumerSequenceManager, ExclusiveConsumerLockManager lockManager) {
        this.storage = storage;
        this.sharedMessageReader = new SharedMessageReader(storage, consumerSequenceManager, config);
        this.exclusiveMessageReader = new ExclusiveMessageReader(storage, consumerSequenceManager, lockManager, config);
    }

    public ReceiveResult putMessage(final ReceivingMessage message) {
        final RawMessage rawMessage = message.getMessage();
        final MessageHeader header = rawMessage.getHeader();
        final String msgId = header.getMessageId();
        final long start = System.currentTimeMillis();
        try {
            final PutMessageResult putMessageResult = storage.appendMessage(rawMessage);
            final PutMessageStatus status = putMessageResult.getStatus();
            if (status != PutMessageStatus.SUCCESS) {
                LOGGER.error("put message error, message:{} {}, status:{}", header.getSubject(), msgId, status.name());
                QMon.storeMessageErrorCountInc(header.getSubject());
                return new ReceiveResult(msgId, MessageProducerCode.STORE_ERROR, status.name(), -1);
            }

            AppendMessageResult<MessageSequence> result = putMessageResult.getResult();
            final long endOffsetOfMessage = result.getWroteOffset() + result.getWroteBytes();
            return new ReceiveResult(msgId, MessageProducerCode.SUCCESS, "", endOffsetOfMessage);
        } catch (Throwable e) {
            LOGGER.error("put message error, message:{} {}", header.getSubject(), header.getMessageId(), e);
            QMon.storeMessageErrorCountInc(header.getSubject());
            return new ReceiveResult(msgId, MessageProducerCode.STORE_ERROR, "", -1);
        } finally {
            QMon.putMessageTime(header.getSubject(), System.currentTimeMillis() - start);
        }
    }

    public PullMessageResult findMessages(final PullRequest pullRequest) {
        if (pullRequest.isExclusiveConsume()) {
            return exclusiveMessageReader.findMessages(pullRequest);
        }
        return sharedMessageReader.findMessages(pullRequest);
    }

    public long getQueueCount(String subject, String group) {
        return storage.locateConsumeQueue(subject, group).getQueueCount();
    }
}
