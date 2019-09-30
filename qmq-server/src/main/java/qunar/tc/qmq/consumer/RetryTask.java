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

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumerSequence;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.action.RangeAckAction;

/**
 * Created by zhaohui.yu
 * 7/30/18
 */
class RetryTask {
    private static final Logger LOG = LoggerFactory.getLogger(RetryTask.class);

    private final DynamicConfig config;
    private final ConsumerSequenceManager consumerSequenceManager;
    private final Subscriber subscriber;
    private final RateLimiter limiter;

    private volatile boolean cancel;

    RetryTask(DynamicConfig config, ConsumerSequenceManager consumerSequenceManager, Subscriber subscriber) {

        this.config = config;
        this.consumerSequenceManager = consumerSequenceManager;
        this.subscriber = subscriber;
        this.limiter = RateLimiter.create(50);
        this.config.addListener(conf -> updateLimitRate(conf, "put_need_retry_message.limiter"));
    }

    private void updateLimitRate(DynamicConfig conf, final String key) {
        if (!conf.exist(key)) {
            return;
        }

        try {
            final double limit = conf.getDouble(key);
            limiter.setRate(limit);
        } catch (Exception e) {
            LOG.debug("update limiter rate failed", e);
        }
    }

    void run() {
        if (cancel) return;

        final ConsumerSequence consumerSequence = consumerSequenceManager.getConsumerSequence(subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId());
        if (consumerSequence == null) {
            return;
        }

        QMon.retryTaskExecuteCountInc(subscriber.getSubject(), subscriber.getGroup());
        while (true) {
            limiter.acquire();

            if (!consumerSequence.tryLock()) return;
            try {
                if (cancel) return;

                final long firstNotAckedSequence = consumerSequence.getAckSequence() + 1;
                final long lastPulledSequence = consumerSequence.getPullSequence();
                if (lastPulledSequence < firstNotAckedSequence) return;


                subscriber.renew();

                LOG.info("put need retry message in retry task, subject: {}, group: {}, consumerId: {}, ack offset: {}, pull offset: {}",
                        subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId(), firstNotAckedSequence, lastPulledSequence);
                consumerSequenceManager.putNeedRetryMessages(subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId(), firstNotAckedSequence, firstNotAckedSequence);

                // put ack action
                final Action action = new RangeAckAction(subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId(), System.currentTimeMillis(), firstNotAckedSequence, firstNotAckedSequence);
                if (consumerSequenceManager.putAction(action)) {
                    consumerSequence.setAckSequence(firstNotAckedSequence);
                    QMon.consumerAckTimeoutErrorCountInc(subscriber.getConsumerId(), 1);
                }
            } finally {
                consumerSequence.unlock();
            }
        }
    }

    void cancel() {
        cancel = true;
    }

    void reset() {
        cancel = false;
    }
}
