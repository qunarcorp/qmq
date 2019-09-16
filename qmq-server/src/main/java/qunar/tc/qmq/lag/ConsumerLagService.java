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

package qunar.tc.qmq.lag;

import qunar.tc.qmq.base.ConsumerLag;
import qunar.tc.qmq.store.ConsumeQueue;
import qunar.tc.qmq.store.ConsumerGroupProgress;
import qunar.tc.qmq.store.ConsumerProgress;
import qunar.tc.qmq.store.Storage;

import java.util.HashMap;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2018/7/31
 */
public class ConsumerLagService {
    private final Storage storage;

    public ConsumerLagService(final Storage storage) {
        this.storage = storage;
    }

    public Map<String, ConsumerLag> getSubjectConsumerLag(final String subject) {
        final Map<String, ConsumeQueue> consumeQueues = storage.locateSubjectConsumeQueues(subject);
        final Map<String, ConsumerLag> lags = new HashMap<>();
        for (final Map.Entry<String, ConsumeQueue> entry : consumeQueues.entrySet()) {
            final String consumerGroup = entry.getKey();
            final ConsumeQueue consumeQueue = entry.getValue();
            final ConsumerGroupProgress progress = storage.getConsumerGroupProgress(subject, consumerGroup);

            final long pullLag = consumeQueue.getQueueCount();
            final long ackLag = computeAckLag(progress);
            lags.put(consumerGroup, new ConsumerLag(pullLag, ackLag));
        }
        return lags;
    }


    private long computeAckLag(final ConsumerGroupProgress progress) {
        if (progress == null) {
            return 0;
        }

        final Map<String, ConsumerProgress> consumers = progress.getConsumers();
        if (consumers == null || consumers.isEmpty()) {
            return progress.getPull();
        }

        long totalAckLag = 0;
        for (final ConsumerProgress consumer : consumers.values()) {
            // TODO(keli.wang): check if pull - ack is the right lag
            final long ackLag = consumer.getPull() - consumer.getAck();
            totalAckLag += ackLag;
        }
        return totalAckLag;
    }
}
