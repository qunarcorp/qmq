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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * @author keli.wang
 * @since 2017/7/31
 */
public class ConsumeQueueManager {

    private final Logger LOGGER = LoggerFactory.getLogger(ConsumeQueueManager.class);

    private final Table<String, String, ConsumeQueue> queues;
    private final Storage storage;

    public ConsumeQueueManager(final Storage storage) {
        this.queues = HashBasedTable.create();
        this.storage = storage;
    }

    public synchronized ConsumeQueue getOrCreate(final String subject, final String group) {
        if (!queues.contains(subject, group)) {
            Optional<Long> lastMaxSequence = getLastMaxSequence(subject, group);

            OffsetBound subjectConsumerLogBound = storage.getSubjectConsumerLogBound(subject);
            long maxSequence = subjectConsumerLogBound == null ? 0 : subjectConsumerLogBound.getMaxOffset();
            if (!lastMaxSequence.isPresent()) {
                LOGGER.info("发现新的group：{} 订阅了subject: {},从最新的消息开始消费, 最新的sequence为：{}！", group, subject, maxSequence);
            }
            final long nextSequence = lastMaxSequence.map(seq -> seq + 1)
                    .orElse(maxSequence);
            queues.put(subject, group, new ConsumeQueue(storage, subject, group, nextSequence));
        }
        return queues.get(subject, group);
    }

    public synchronized Map<String, ConsumeQueue> getBySubject(final String subject) {
        if (queues.containsRow(subject)) {
            return queues.row(subject);
        } else {
            return Collections.emptyMap();
        }
    }

    private Optional<Long> getLastMaxSequence(final String subject, final String group) {
        final ConsumerGroupProgress progress = storage.getConsumerGroupProgress(subject, group);
        if (progress == null) {
            return Optional.empty();
        } else {
            return Optional.of(progress.getPull());
        }
    }

    public synchronized void update(final String subject, final String group, final long nextSequence) {
        final ConsumeQueue queue = getOrCreate(subject, group);
        queue.setNextSequence(nextSequence);
    }

    synchronized void disableLagMonitor(String subject, String group) {
        final ConsumeQueue consumeQueue = queues.get(subject, group);
        if (consumeQueue == null) {
            return;
        }
        consumeQueue.disableLagMonitor(subject, group);
    }
}
