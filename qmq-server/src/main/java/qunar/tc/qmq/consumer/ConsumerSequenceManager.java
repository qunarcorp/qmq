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

package qunar.tc.qmq.consumer;

import com.google.common.collect.Table;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumerGroup;
import qunar.tc.qmq.base.ConsumerSequence;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.base.WritePutActionResult;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.processor.AckMessageProcessor;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.store.action.ForeverOfflineAction;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yunfeng.yang
 * @since 2017/8/1
 */
public class ConsumerSequenceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerSequenceManager.class);

    private static final long ACTION_LOG_ORIGIN_OFFSET = -1L;

    private final Storage storage;

    // for share consume: consumerId -> ((subject + consumerGroup) -> sequence)
    // for exclusive consume: consumerGroup -> ((subject + consumerGroup) -> sequence)
    private final ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ConsumerSequence>> sequences;

    public ConsumerSequenceManager(final Storage storage) {
        this.storage = storage;
        this.sequences = new ConcurrentHashMap<>();
    }

    public void init() {
        loadFromConsumerGroupProgresses(sequences);
    }

    private void loadFromConsumerGroupProgresses(final ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ConsumerSequence>> result) {
        final Collection<ConsumerGroupProgress> progresses = storage.allConsumerGroupProgresses().values();
        progresses.forEach(progress -> {
            final Map<String, ConsumerProgress> consumers = progress.getConsumers();
            if (consumers == null || consumers.isEmpty()) {
                return;
            }

            consumers.values().forEach(consumer -> putConsumer(result, consumer));
        });
    }

    private void putConsumer(final ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ConsumerSequence>> result, final ConsumerProgress consumer) {
        final String consumerId = consumer.getConsumerId();
        ConcurrentMap<ConsumerGroup, ConsumerSequence> consumerSequences = result.computeIfAbsent(consumerId, (k) -> new ConcurrentHashMap<>());
        final ConsumerSequence consumerSequence = new ConsumerSequence(consumer.getPull(), consumer.getAck());
        final ConsumerGroup consumerGroup = new ConsumerGroup(consumer.getSubject(), consumer.getGroup());
        consumerSequences.putIfAbsent(consumerGroup, consumerSequence);

    }

    /**
     * 只有共享模式消费才需要记录pull log了
     *
     * @param subject          消息主题(实际应该是partition name)
     * @param consumerGroup    消费者分组
     * @param consumerId       消费者唯一id
     * @param getMessageResult 从Storage里查出来的消息
     * @return
     */
    public WritePutActionResult putPullActions(final String subject,
                                               final String consumerGroup,
                                               final String consumerId,
                                               final boolean isExclusiveConsume,
                                               final GetMessageResult getMessageResult) {
        final OffsetRange consumerLogRange = getMessageResult.getConsumerLogRange();
        final ConsumerSequence consumerSequence = getOrCreateConsumerSequence(subject, consumerGroup, consumerId, isExclusiveConsume);

        if (consumerLogRange.getEnd() - consumerLogRange.getBegin() + 1 != getMessageResult.getMessageNum()) {
            LOGGER.debug("consumer offset range error, subject:{}, consumerGroup:{}, consumerId:{}, isExclusiveConsume:{}, getMessageResult:{}",
                    subject, consumerGroup, consumerId, false, getMessageResult);
            QMon.consumerLogOffsetRangeError(subject, consumerGroup);
        }
        consumerSequence.pullLock();
        try {
            //因为消息堆积等原因，可能会导致历史消息已经被删除了。所以可能得出这种情况：一次拉取100条消息，然后前20条已经删除了，所以不能使用begin，要使用end减去消息条数这种方式
            final long firstConsumerLogSequence = consumerLogRange.getEnd() - getMessageResult.getMessageNum() + 1;
            final long lastConsumerLogSequence = consumerLogRange.getEnd();

            final long firstPullSequence = consumerSequence.getPullSequence() + 1;
            final long lastPullSequence = consumerSequence.getPullSequence() + getMessageResult.getMessageNum();

            final Action action = new PullAction(subject, consumerGroup, consumerId,
                    System.currentTimeMillis(), isExclusiveConsume,
                    firstPullSequence, lastPullSequence,
                    firstConsumerLogSequence, lastConsumerLogSequence);

            if (!putAction(action)) {
                return new WritePutActionResult(false, -1);
            }
            consumerSequence.setPullSequence(lastPullSequence);
            return new WritePutActionResult(true, firstPullSequence);
        } catch (Exception e) {
            LOGGER.error("write action log failed, subject: {}, consumerGroup: {}, consumerId: {}", subject, consumerGroup, consumerId, e);
            return new WritePutActionResult(false, -1);
        } finally {
            consumerSequence.pullUnlock();
        }
    }

    public boolean putAckActions(AckMessageProcessor.AckEntry ackEntry) {
        final String partitionName = ackEntry.getPartitionName();
        final String consumerGroup = ackEntry.getConsumerGroup();
        final String consumerId = ackEntry.getConsumerId();
        final long lastPullSequence = ackEntry.getLastPullLogOffset();
        final boolean isExclusiveConsume = ackEntry.isExclusiveConsume();

        long firstPullSequence = ackEntry.getFirstPullLogOffset();

        final ConsumerSequence consumerSequence = getOrCreateConsumerSequence(partitionName, consumerGroup, consumerId, isExclusiveConsume);
        consumerSequence.ackLock();
        final long confirmedAckSequence = consumerSequence.getAckSequence();
        try {
            //         end sequence of current ack range                      confirm ack sequence
            //   -----------------------|------------------------------------------------|----------
            // ack已经ack过的消息
            if (lastPullSequence <= confirmedAckSequence) {
                LOGGER.warn("receive duplicate ack, ackEntry:{}, consumerSequence:{} ", ackEntry, consumerSequence);
                QMon.consumerDuplicateAckCountInc(partitionName, consumerGroup, (int) (confirmedAckSequence - lastPullSequence));
                return true;
            }
            final long lostAckCount = firstPullSequence - confirmedAckSequence;
            if (lostAckCount <= 0) {
                LOGGER.warn("receive some duplicate ack, ackEntry:{}, consumerSequence:{}", ackEntry, consumerSequence);
                firstPullSequence = confirmedAckSequence + 1;
                QMon.consumerDuplicateAckCountInc(partitionName, consumerGroup, (int) (confirmedAckSequence - firstPullSequence));
            } else if (lostAckCount > 1) {
                final long firstNotAckedPullSequence = confirmedAckSequence + 1;
                final long lastLostPullSequence = firstPullSequence - 1;
                //如果是独占消费，put need retry也是没有意义的
                if (!isExclusiveConsume) {
                    LOGGER.error("lost ack count, ackEntry:{}, consumerSequence:{}", ackEntry, consumerSequence);
                    putNeedRetryMessages(partitionName, consumerGroup, consumerId, firstNotAckedPullSequence, lastLostPullSequence);
                }
                firstPullSequence = firstNotAckedPullSequence;
                QMon.consumerLostAckCountInc(partitionName, consumerGroup, (int) lostAckCount);
            }

            final Action rangeAckAction = new RangeAckAction(partitionName, consumerGroup, consumerId,
                    System.currentTimeMillis(), isExclusiveConsume, firstPullSequence, lastPullSequence);
            if (!putAction(rangeAckAction))
                return false;

            consumerSequence.setAckSequence(lastPullSequence);
            return true;
        } catch (Exception e) {
            QMon.putAckActionsErrorCountInc(ackEntry.getPartitionName(), ackEntry.getConsumerGroup());
            LOGGER.error("put ack actions error, ackEntry:{}, consumerSequence:{}", ackEntry, consumerSequence, e);
            return false;
        } finally {
            consumerSequence.ackUnLock();
        }
    }

    boolean putForeverOfflineAction(final String subject, final String consumerGroup, final String consumerId) {
        final ForeverOfflineAction action = new ForeverOfflineAction(subject, consumerGroup, consumerId, System.currentTimeMillis());
        return putAction(action);
    }

    public boolean putAction(final Action action) {
        final PutMessageResult putMessageResult = storage.putAction(action);
        if (putMessageResult.getStatus() == PutMessageStatus.SUCCESS) {
            return true;
        }

        LOGGER.error("put action fail, action:{}", action);
        QMon.putActionFailedCountInc(action.partitionName(), action.consumerGroup());
        return false;
    }

    void putNeedRetryMessages(String partitionName, String consumerGroup, String consumerId, long firstNotAckedOffset, long lastPullLogOffset) {
        if (noPullLog(partitionName, consumerGroup, consumerId)) return;

        // get error msg
        final List<Buffer> needRetryMessages = getNeedRetryMessages(partitionName, consumerGroup, consumerId, firstNotAckedOffset, lastPullLogOffset);
        // put error msg
        putNeedRetryMessages(partitionName, consumerGroup, consumerId, needRetryMessages);
    }

    private boolean noPullLog(String partitionName, String consumerGroup, String consumerId) {
        Table<String, String, PullLog> pullLogs = storage.allPullLogs();
        Map<String, PullLog> subscribers = pullLogs.row(consumerId);
        if (subscribers == null || subscribers.isEmpty()) return true;
        return subscribers.get(GroupAndPartition.groupAndPartition(partitionName, consumerGroup)) == null;
    }

    void remove(String subject, String consumerGroup, String consumerId) {
        final ConcurrentMap<ConsumerGroup, ConsumerSequence> consumers = sequences.get(consumerId);
        if (consumers == null) return;

        consumers.remove(new ConsumerGroup(subject, consumerGroup));
        if (consumers.isEmpty()) {
            sequences.remove(consumerId);
        }
    }

    private List<Buffer> getNeedRetryMessages(String partitionName, String consumerGroup, String consumerId, long firstNotAckedSequence, long lastPullSequence) {
        final int actualNum = (int) (lastPullSequence - firstNotAckedSequence + 1);
        final List<Buffer> needRetryMessages = new ArrayList<>(actualNum);
        for (long sequence = firstNotAckedSequence; sequence <= lastPullSequence; sequence++) {
            final long consumerLogSequence = storage.getMessageSequenceByPullLog(partitionName, consumerGroup, consumerId, sequence);
            if (consumerLogSequence < 0) {
                LOGGER.warn("find no consumer log offset for this pull log, partitionName:{}, consumerGroup:{}, consumerId:{}, sequence:{}, consumerLogSequence:{}", partitionName, consumerGroup, consumerId, sequence, consumerLogSequence);
                continue;
            }

            final GetMessageResult getMessageResult = storage.getMessage(partitionName, consumerLogSequence);
            if (getMessageResult.getStatus() == GetMessageStatus.SUCCESS) {
                final List<Buffer> buffers = getMessageResult.getBuffers();
                needRetryMessages.addAll(buffers);
            }
        }
        return needRetryMessages;
    }

    private void putNeedRetryMessages(String partitionName, String consumerGroup, String consumerId, List<Buffer> needRetryMessages) {
        try {
            for (Buffer buffer : needRetryMessages) {
                final ByteBuf message = Unpooled.wrappedBuffer(buffer.getBuffer());
                final RawMessage rawMessage = QMQSerializer.deserializeRawMessage(message);
                if (!RetryPartitionUtils.isRetryPartitionName(partitionName)) {
                    final String retrySubject = RetryPartitionUtils.buildRetryPartitionName(partitionName, consumerGroup);
                    rawMessage.setSubject(retrySubject);
                }

                final PutMessageResult putMessageResult = storage.appendMessage(rawMessage);
                if (putMessageResult.getStatus() != PutMessageStatus.SUCCESS) {
                    LOGGER.error("put message error, consumer:{} {} {}, status:{}", partitionName, consumerGroup, consumerId, putMessageResult.getStatus());
                    throw new RuntimeException("put retry message error");
                }
            }
        } finally {
            needRetryMessages.forEach(Buffer::release);
        }

        QMon.putNeedRetryMessagesCountInc(partitionName, consumerGroup, needRetryMessages.size());
    }

    public ConsumerSequence getConsumerSequence(String partitionName, String consumerGroup, String consumerId) {
        final ConcurrentMap<ConsumerGroup, ConsumerSequence> consumerSequences = this.sequences.get(consumerId);
        if (consumerSequences == null) {
            return null;
        }
        return consumerSequences.get(new ConsumerGroup(partitionName, consumerGroup));
    }

    public ConsumerSequence getOrCreateConsumerSequence(String partitionName, String consumerGroup, String consumerId, boolean isExclusiveConsume) {
        String exclusiveKey = isExclusiveConsume ? consumerGroup : consumerId;
        ConcurrentMap<ConsumerGroup, ConsumerSequence> consumerSequences = sequences.computeIfAbsent(exclusiveKey, k -> new ConcurrentHashMap<>());
        final ConsumerGroup consumerGroupKey = new ConsumerGroup(partitionName, consumerGroup);
        return consumerSequences.computeIfAbsent(consumerGroupKey, k -> new ConsumerSequence(ACTION_LOG_ORIGIN_OFFSET, ACTION_LOG_ORIGIN_OFFSET));
    }
}
