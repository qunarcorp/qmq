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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.metrics.Metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-7-20.
 */
public class AckEntry {
    private static final Logger LOGGER = LoggerFactory.getLogger(AckEntry.class);

    private final AckSendQueue ackSendQueue;
    private final DelayMessageService delayMessageService;

    private final long pullOffset;

    private final AtomicBoolean completing = new AtomicBoolean(false);
    private volatile boolean done = false;
    private volatile AckEntry next;

    AckEntry(AckSendQueue ackSendQueue, long pullOffset, DelayMessageService delayMessageService) {
        this.ackSendQueue = ackSendQueue;
        this.pullOffset = pullOffset;
        this.delayMessageService = delayMessageService;
    }

    void setNext(AckEntry next) {
        this.next = next;
    }

    AckEntry next() {
        return next;
    }

    long pullOffset() {
        return pullOffset;
    }

    public void ack() {
        if (!completing.compareAndSet(false, true)) {
            return;
        }
        completed();
    }

    public void nack(final int nextRetryCount, final BaseMessage message) {
        if (!completing.compareAndSet(false, true)) {
            return;
        }
        doSendNack(nextRetryCount, message);
    }

    private void doSendNack(final int nextRetryCount, final BaseMessage message) {
        while (true) {
            try {
                ackSendQueue.sendBackAndCompleteNack(nextRetryCount, message, this);
                return;
            } catch (Exception e) {
                LOGGER.warn("nack exception. subject={}, group={}", ackSendQueue.getSubject(), ackSendQueue.getConsumerGroup(), e);
                Metrics.counter("qmq_pull_sendNack_error", SUBJECT_GROUP_ARRAY, new String[]{message.getSubject(), ackSendQueue.getConsumerGroup()}).inc();
            }
        }
    }

    public void ackDelay(int nextRetryCount, long nextRetryTime, BaseMessage message) {
        if (!completing.compareAndSet(false, true)) return;

        try {
            if (delayMessageService.sendDelayMessage(nextRetryCount, nextRetryTime, message, ackSendQueue.getConsumerGroup())) {
                completed();
                LOGGER.info("send delay message: " + message.getMessageId());
                return;
            }
            Metrics.counter("qmq_pull_sendAckDelay_error", SUBJECT_GROUP_ARRAY, new String[]{message.getSubject(), ackSendQueue.getConsumerGroup()}).inc();
        } catch (Exception e) {
            LOGGER.error("发送延迟消息失败，改成发送nack. subject={}, messageId={}", message.getSubject(), message.getMessageId(), e);
            Metrics.counter("qmq_pull_sendAckDelay_error", SUBJECT_GROUP_ARRAY, new String[]{message.getSubject(), ackSendQueue.getConsumerGroup()}).inc();
        }


        doSendNack(nextRetryCount, message);
    }

    void completed() {
        done = true;
        ackSendQueue.ackCompleted(this);
    }

    boolean isDone() {
        return done;
    }
}