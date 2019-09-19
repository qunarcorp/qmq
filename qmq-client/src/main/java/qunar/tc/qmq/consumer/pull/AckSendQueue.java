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

package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.common.TimerUtil;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqMeter;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-23.
 */
class AckSendQueue implements TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(AckSendQueue.class);
    private static final long DEFAULT_PULL_OFFSET = -1;
    private static final int ACK_INTERVAL_SECONDS = 10;
    private static final long ACK_TRY_SEND_TIMEOUT_MILLIS = 1000;

    private static final int DESTROY_CHECK_WAIT_MILLIS = 50;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final String brokerGroupName;
    private final String subject;
    private final String consumerGroup;
    private final boolean isBroadcast;
    private final boolean isOrdered;

    private final AckService ackService;

    private final String partitionName;
    private final String retryPartitionName;
    private final String deadRetryPartitionName;
    private final ConsumeStrategy consumeStrategy;

    private final AtomicReference<Integer> pullBatchSize;

    private final ReentrantLock updateLock = new ReentrantLock();

    private final AtomicLong minPullOffset = new AtomicLong(DEFAULT_PULL_OFFSET);
    private final AtomicLong maxPullOffset = new AtomicLong(DEFAULT_PULL_OFFSET);

    private final AtomicInteger toSendNum = new AtomicInteger(0);
    private QmqMeter sendNumQps;
    private QmqCounter appendErrorCount;
    private QmqCounter sendErrorCount;
    private QmqCounter sendFailCount;
    private QmqCounter deadQueueCount;

    private final LinkedBlockingQueue<AckSendEntry> sendEntryQueue = new LinkedBlockingQueue<>();
    private final ReentrantLock sendLock = new ReentrantLock();
    private final AtomicBoolean inSending = new AtomicBoolean(false);

    private final BrokerService brokerService;
    private final SendMessageBack sendMessageBack;

    private final RateLimiter ackSendFailLogLimit = RateLimiter.create(0.5);

    private volatile AckEntry head = null;
    private volatile AckEntry tail = null;
    private volatile AckEntry beginScanPosition = null;

    private volatile long lastAppendOffset = -1;
    private volatile long lastSendOkOffset = -1;

    AckSendQueue(
            String subject,
            String consumerGroup,
            String partitionName,
            String brokerGroupName,
            ConsumeStrategy consumeStrategy,
            AckService ackService,
            BrokerService brokerService,
            SendMessageBack sendMessageBack,
            boolean isBroadcast,
            boolean isOrdered
    ) {
        this.brokerGroupName = brokerGroupName;
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.consumeStrategy = consumeStrategy;
        this.ackService = ackService;
        this.brokerService = brokerService;
        this.sendMessageBack = sendMessageBack;
        this.isBroadcast = isBroadcast;
        this.isOrdered = isOrdered;

        this.partitionName = RetryPartitionUtils.getRealPartitionName(partitionName);
        this.retryPartitionName = RetryPartitionUtils.buildRetryPartitionName(partitionName, consumerGroup);
        this.deadRetryPartitionName = RetryPartitionUtils.buildDeadRetryPartitionName(partitionName, consumerGroup);
        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(subject);
    }

    void append(final List<AckEntry> batch) {
        if (batch == null || batch.isEmpty() || stopped.get()) return;

        updateLock.lock();
        try {
            if (lastAppendOffset != -1 && lastAppendOffset + 1 != batch.get(0).pullOffset()) {
                LOGGER.warn("{}/{} append ack entry not continous. last: {}, new: {}", subject, consumerGroup, lastAppendOffset, batch.get(0).pullOffset());
                appendErrorCount.inc();
            }

            if (head == null) {
                beginScanPosition = head = batch.get(0);
                minPullOffset.set(head.pullOffset());
            }

            if (tail != null) {
                tail.setNext(batch.get(0));
            }

            tail = batch.get(batch.size() - 1);
            lastAppendOffset = tail.pullOffset();
            maxPullOffset.set(tail.pullOffset());
            toSendNum.getAndAdd(batch.size());
        } finally {
            updateLock.unlock();
        }
    }

    void sendBackAndCompleteNack(final int nextRetryCount, final BaseMessage message, final AckEntry ackEntry) {
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(subject);
        boolean isDeadRetryMessage = orderStrategy.isDeadRetry(nextRetryCount, message);
        final String partitionName = isDeadRetryMessage ? deadRetryPartitionName : retryPartitionName;
        if (deadRetryPartitionName.equals(partitionName)) {
            deadQueueCount.inc();
            LOGGER.warn("process message retry num {} >= {}, and dead retry. subject={}, group={}, msgId={}",
                    nextRetryCount - 1, message.getMaxRetryNum(), this.subject, consumerGroup, message.getMessageId());
        }
        message.setPartitionName(partitionName);
        sendMessageBack.sendBackAndCompleteNack(nextRetryCount, message, ackEntry);
    }

    void ackCompleted(AckEntry current) {
        if (current == null) return;

        updateLock.lock();
        try {
            if (beginScanPosition == null || beginScanPosition.pullOffset() != current.pullOffset()) {
                return;
            }

            AckEntry end = scanCompleted(current);
            beginScanPosition = end.next();
            if (allowSendAck(end)) {
                final AckSendEntry sendEntry = new AckSendEntry(head, end, isBroadcast);
                head = beginScanPosition;
                if (head == null) {
                    tail = null;
                }
                sendEntryQueue.offer(sendEntry);
            } else {
                return;
            }
        } finally {
            updateLock.unlock();
        }
        sendAck();
    }

    private boolean allowSendAck(AckEntry needAck) {
        return needAck.next() == null || needAck.pullOffset() - head.pullOffset() >= pullBatchSize.get() - 1;
    }

    private AckEntry scanCompleted(AckEntry begin) {
        AckEntry needAck = begin;
        while (needAck.next() != null && needAck.next().isDone()) {
            needAck = needAck.next();
        }
        return needAck;
    }

    boolean trySendAck(long timeout) {
        if (!tryLock(timeout)) return false;

        try {
            if (head == null || !head.isDone()) {
                return sendAck();
            }

            AckEntry end = scanCompleted(head);

            final AckSendEntry sendEntry = new AckSendEntry(head, end, isBroadcast);
            head = beginScanPosition = end.next();

            if (head == null) {
                tail = null;
            }
            sendEntryQueue.offer(sendEntry);
        } finally {
            updateLock.unlock();
        }
        return sendAck();
    }

    private boolean tryLock(long timeout) {
        try {
            if (!updateLock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    private boolean sendAck() {
        AckSendEntry sendEntry;

        if (inSending.get()) return false;
        sendLock.lock();
        try {
            if (inSending.get() || sendEntryQueue.isEmpty()) return false;
            sendEntry = sendEntryQueue.peek();
            if (sendEntry != null) {
                inSending.set(true);
            } else {
                sendEntryQueue.poll();
                LOGGER.error("sendEntry is null");
                return false;
            }
        } finally {
            sendLock.unlock();
        }

        doSendAck(sendEntry);
        return true;
    }

    private void doSendAck(final AckSendEntry sendEntry) {
        BrokerGroupInfo brokerGroup = getBrokerGroup();
        if (brokerGroup == null) {
            LOGGER.debug("lost broker group: {}. subject={}, consumeGroup={}", brokerGroupName, subject, consumerGroup);
            inSending.set(false);
            return;
        }

        ackService.sendAck(brokerGroup, subject, consumerGroup, consumeStrategy, sendEntry, new AckService.SendAckCallback() {
            @Override
            public void success() {
                if (lastSendOkOffset != -1 && lastSendOkOffset + 1 != sendEntry.getPullOffsetBegin()) {
                    LOGGER.warn("{}/{} ack send not continous. last={}, send={}", subject, consumerGroup, lastSendOkOffset, sendEntry);
                    sendErrorCount.inc();
                }
                lastSendOkOffset = sendEntry.getPullOffsetLast();

                minPullOffset.set(sendEntry.getPullOffsetLast() + 1);
                final int sendNum = (int) (sendEntry.getPullOffsetLast() - sendEntry.getPullOffsetBegin()) + 1;
                toSendNum.getAndAdd(-sendNum);
                sendNumQps.mark(sendNum);

                AckSendEntry head = sendEntryQueue.peek();
                if (head == null || head.getPullOffsetBegin() != sendEntry.getPullOffsetBegin()) {
                    LOGGER.error("ack send error: {}, {}", sendEntry, head);
                    sendErrorCount.inc();
                } else {
                    LOGGER.debug("AckSendRet ok [{}, {}]", sendEntry.getPullOffsetBegin(), sendEntry.getPullOffsetLast());
                    sendEntryQueue.poll();
                }

                inSending.set(false);
                AckSendQueue.this.sendAck();
            }

            @Override
            public void fail(Exception ex) {
                if (ackSendFailLogLimit.tryAcquire()) {
                    LOGGER.warn("send ack fail, will retry next", ex);
                }
                LOGGER.debug("AckSendRet fail [{}, {}]", sendEntry.getPullOffsetBegin(), sendEntry.getPullOffsetLast());
                sendFailCount.inc();
                inSending.set(false);
            }
        });
    }

    private BrokerGroupInfo getBrokerGroup() {
        return brokerService.getBrokerCluster(ClientType.CONSUMER, subject, consumerGroup, isBroadcast, isOrdered).getGroupByName(brokerGroupName);
    }

    AckSendInfo getAckSendInfo() {
        AckSendInfo info = new AckSendInfo();
        info.setMinPullOffset(minPullOffset.get());
        info.setMaxPullOffset(maxPullOffset.get());
        info.setToSendNum(toSendNum.get());
        return info;
    }


    void init() {
        TimerUtil.newTimeout(this, ACK_INTERVAL_SECONDS, TimeUnit.SECONDS);

        String[] values = new String[]{subject, consumerGroup};
        sendNumQps = Metrics.meter("qmq_pull_ack_sendnum_qps", SUBJECT_GROUP_ARRAY, values);
        appendErrorCount = Metrics.counter("qmq_pull_ack_appenderror_count", SUBJECT_GROUP_ARRAY, values);
        sendErrorCount = Metrics.counter("qmq_pull_ack_senderror_count", SUBJECT_GROUP_ARRAY, values);
        sendFailCount = Metrics.counter("qmq_pull_ack_sendfail_count", SUBJECT_GROUP_ARRAY, values);
        deadQueueCount = Metrics.counter("qmq_deadqueue_send_count", SUBJECT_GROUP_ARRAY, values);

        Metrics.gauge("qmq_pull_ack_min_offset", SUBJECT_GROUP_ARRAY, values, new Supplier<Double>() {
            @Override
            public Double get() {
                return (double) minPullOffset.get();
            }
        });
        Metrics.gauge("qmq_pull_ack_max_offset", SUBJECT_GROUP_ARRAY, values, new Supplier<Double>() {
            @Override
            public Double get() {
                return (double) maxPullOffset.get();
            }
        });
        Metrics.gauge("qmq_pull_ack_tosendnum", SUBJECT_GROUP_ARRAY, values, new Supplier<Double>() {
            @Override
            public Double get() {
                return (double) toSendNum.get();
            }
        });
    }

    public String getSubject() {
        return subject;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    private static final AckSendEntry EMPTY_ACK = new AckSendEntry();

    private static final AckService.SendAckCallback EMPTY_ACK_CALLBACK = new AckService.SendAckCallback() {
        @Override
        public void success() {
            LOGGER.debug("send heartbeat ok");
        }

        @Override
        public void fail(Exception ex) {
            LOGGER.error("send heartbeat fail", ex);
        }
    };

    @Override
    public void run(Timeout timeout) {
        try {
            if (!trySendAck(ACK_TRY_SEND_TIMEOUT_MILLIS)) {
                final BrokerGroupInfo brokerGroup = getBrokerGroup();
                if (brokerGroup == null) {
                    LOGGER.debug("lost broker group: {}. subject={}, consumeGroup={}", brokerGroupName, subject, consumerGroup);
                    return;
                }
                ackService.sendAck(brokerGroup, subject, consumerGroup, consumeStrategy, EMPTY_ACK, EMPTY_ACK_CALLBACK);
            }
        } finally {
            TimerUtil.newTimeout(this, ACK_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    public void destroy(long waitTimeMills) {
        trySendAck(waitTimeMills);
        while (waitTimeMills > 0 && toSendNum.get() > 0) {
            try {
                Thread.sleep(DESTROY_CHECK_WAIT_MILLIS);
            } catch (Exception e) {
                break;
            }
            waitTimeMills -= DESTROY_CHECK_WAIT_MILLIS;
        }
        stopped.set(true);
    }
}
