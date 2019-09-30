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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumerSequence;
import qunar.tc.qmq.monitor.QMon;

/**
 * Created by zhaohui.yu
 * 7/31/18
 */
class OfflineTask {
    private static final Logger LOG = LoggerFactory.getLogger(OfflineTask.class);

    private final ConsumerSequenceManager consumerSequenceManager;
    private final Subscriber subscriber;

    private volatile boolean cancel = false;

    OfflineTask(ConsumerSequenceManager consumerSequenceManager, Subscriber subscriber) {
        this.consumerSequenceManager = consumerSequenceManager;
        this.subscriber = subscriber;
    }

    void run() {
        if (cancel) return;

        LOG.info("run offline task for {}/{}/{}.", subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId());
        QMon.offlineTaskExecuteCountInc(subscriber.getSubject(), subscriber.getGroup());

        final ConsumerSequence consumerSequence = consumerSequenceManager.getOrCreateConsumerSequence(subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId(), false);
        if (!consumerSequence.tryLock()) return;
        try {
            if (cancel) return;

            if (isProcessedComplete(consumerSequence)) {
                if (unSubscribe()) {
                    LOG.info("offline task destroyed subscriber for {}/{}/{}", subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId());
                }
            } else {
                LOG.info("offline task skip destroy subscriber for {}/{}/{}", subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId());
            }
        } finally {
            consumerSequence.unlock();
        }
    }

    private boolean isProcessedComplete(final ConsumerSequence consumerSequence) {
        final long lastAckedSequence = consumerSequence.getAckSequence();
        final long lastPulledSequence = consumerSequence.getPullSequence();
        return lastPulledSequence <= lastAckedSequence;
    }

    private boolean unSubscribe() {
        if (cancel) return false;
        final boolean success = consumerSequenceManager.putForeverOfflineAction(subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId());
        if (!success) {
            return false;
        }
        consumerSequenceManager.remove(subscriber.getSubject(), subscriber.getGroup(), subscriber.getConsumerId());
        subscriber.destroy();
        return true;
    }

    void cancel() {
        cancel = true;
    }

    void reset() {
        cancel = false;
    }
}
