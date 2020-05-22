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
import qunar.tc.qmq.consumer.BaseMessageHandler;
import qunar.tc.qmq.metrics.Metrics;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @author yiqun.fan create on 17-8-19.
 */
class AckHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(AckHelper.class);

    static void ack(PulledMessage message, Throwable throwable) {
        BaseMessageHandler.printError(message, throwable);
        final AckEntry ackEntry = message.ackEntry();
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

    static void ackWithTrace(PulledMessage message, Throwable throwable) {
        Scope scope = GlobalTracer.get()
                .buildSpan("Qmq.Consume.Ack")
                .withTag("subject", message.getSubject())
                .withTag("messageId", message.getMessageId())
                .withTag("consumerGroup", message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName))
                .startActive(true);
        try {
            ack(message, throwable);
        } catch (Exception e) {
            scope.span().log("ack_failed");
            LOGGER.error("ack exception.", e);
            Metrics.counter("qmq_pull_ackError", SUBJECT_ARRAY, new String[]{message.getSubject()}).inc();
        } finally {
            if (scope != null) {
                scope.close();
            }
        }
    }
}
