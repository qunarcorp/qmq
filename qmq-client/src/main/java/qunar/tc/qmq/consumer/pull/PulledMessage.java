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

import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.ConsumeMessage;
import qunar.tc.qmq.consumer.ConsumerUtils;
import qunar.tc.qmq.metrics.Metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @author yiqun.fan create on 17-7-20.
 */
public class PulledMessage extends ConsumeMessage {

    private static final Logger logger = LoggerFactory.getLogger(PulledMessage.class);

    private transient final AckEntry ackEntry;
    private transient final AckHook ackHook;
    private transient final AtomicBoolean isAcked = new AtomicBoolean(false);

    PulledMessage(BaseMessage message, AckEntry ackEntry, AckHook ackHook) {
        super(message);
        this.ackEntry = ackEntry;
        this.ackHook = ackHook;
    }

    public AckEntry getAckEntry() {
        return ackEntry;
    }

    public boolean isAcked() {
        return isAcked.get();
    }

    public boolean isNotAcked() {
        return !isAcked.get();
    }

    @Override
    public void ack(long elapsed, Throwable e) {
        if (!isAcked.compareAndSet(false, true)) {
            return;
        }
        if (ackHook != null) {
            ackHook.call(this, e);
        } else {
            ack(e);
        }
    }

    public void ack(Throwable throwable) {
        PulledMessage message = this;
        ConsumerUtils.printError(message, throwable);
        final AckEntry ackEntry = message.getAckEntry();
        if (throwable == null) {
            ackEntry.ack();
            return;
        }
        final BaseMessage ackMsg = new BaseMessage(message);
        int nextRetryCount = message.times() + 1;
        ackMsg.setProperty(BaseMessage.keys.qmq_times, nextRetryCount);
        if (throwable instanceof NeedRetryException) {
            ackEntry.ackDelay(nextRetryCount, ((NeedRetryException) throwable).getNext(), ackMsg);
        } else {
            ackEntry.nack(nextRetryCount, ackMsg);
        }
    }

    public void ackWithTrace(Throwable throwable) {
        PulledMessage message = this;
        Scope scope = GlobalTracer.get()
                .buildSpan("Qmq.Consume.Ack")
                .withTag("subject", message.getSubject())
                .withTag("messageId", message.getMessageId())
                .withTag("consumerGroup", message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName))
                .startActive(true);
        try {
            ack(throwable);
        } catch (Exception e) {
            scope.span().log("ack_failed");
            logger.error("ack exception.", e);
            Metrics.counter("qmq_pull_ackError", SUBJECT_ARRAY, new String[]{message.getSubject()}).inc();
        } finally {
            if (scope != null) {
                scope.close();
            }
        }
    }
}
