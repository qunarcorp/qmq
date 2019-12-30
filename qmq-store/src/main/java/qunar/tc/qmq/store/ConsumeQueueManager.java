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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author keli.wang
 * @since 2017/7/31
 */
public class ConsumeQueueManager {

    private final Logger LOGGER = LoggerFactory.getLogger(ConsumeQueueManager.class);

    private final ConcurrentMap<String, ConcurrentMap<String, ConsumeQueue>> queues;
    private final Storage storage;

    public ConsumeQueueManager(final Storage storage) {
        this.queues = new ConcurrentHashMap<>();
        this.storage = storage;
    }

    public ConsumeQueue getOrCreate(final String subject, final String group) {
        ConcurrentMap<String, ConsumeQueue> consumeQueues = queues.computeIfAbsent(subject, s -> new ConcurrentHashMap<>());
        return consumeQueues.computeIfAbsent(group, g -> {
            Optional<Long> lastMaxSequence = getLastMaxSequence(subject, g);
            if (lastMaxSequence.isPresent()) {
                return new ConsumeQueue(storage, subject, g, lastMaxSequence.get() + 1);
            } else {
                OffsetBound consumerLogBound = storage.getSubjectConsumerLogBound(subject);
                long maxSequence = consumerLogBound == null ? 0 : consumerLogBound.getMaxOffset();
                LOGGER.info("发现新的group：{} 订阅了subject: {},从最新的消息开始消费, 最新的sequence为：{}！", g, subject, maxSequence);
                return new ConsumeQueue(storage, subject, g, maxSequence);
            }
        });
    }

    public Map<String, ConsumeQueue> getBySubject(final String subject) {
        ConcurrentMap<String, ConsumeQueue> consumeQueues = queues.get(subject);
        if (consumeQueues != null) {
            return consumeQueues;
        }
        return Collections.emptyMap();
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
        ConcurrentMap<String, ConsumeQueue> consumeQueues = queues.get(subject);
        if (consumeQueues == null) {
            return;
        }
        ConsumeQueue consumeQueue = consumeQueues.get(group);
        if (consumeQueue == null) {
            return;
        }
        consumeQueue.disableLagMonitor(subject, group);
    }
}
