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
import qunar.tc.qmq.monitor.QMon;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static qunar.tc.qmq.store.GroupAndSubject.groupAndSubject;

/**
 * @author keli.wang
 * @since 2017/8/3
 */
public class PullLogManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PullLogManager.class);

    private final StorageConfig config;
    private final Table<String, String, PullLog> logs;
    private final ReadWriteLock logsGuard;

    public PullLogManager(final StorageConfig config, final Table<String, String, ConsumerGroupProgress> consumerGroupProgresses) {
        this.config = config;
        this.logs = HashBasedTable.create();
        this.logsGuard = new ReentrantReadWriteLock();

        loadPullLogs(consumerGroupProgresses);
    }

    private void loadPullLogs(final Table<String, String, ConsumerGroupProgress> consumerGroupProgresses) {
        final File pullLogsRoot = new File(config.getPullLogStorePath());
        final File[] consumerIdDirs = pullLogsRoot.listFiles();
        if (consumerIdDirs == null) {
            return;
        }

        final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final CountDownLatch latch = new CountDownLatch(consumerIdDirs.length);

        for (final File consumerIdDir : consumerIdDirs) {
            executor.submit(() -> {
                try {
                    if (!consumerIdDir.isDirectory()) {
                        return;
                    }
                    loadPullLogsByConsumerId(consumerIdDir, consumerGroupProgresses);
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            LOG.warn("interrupted during shutdown executor", e);
        }
    }

    private void loadPullLogsByConsumerId(final File consumerIdDir, final Table<String, String, ConsumerGroupProgress> consumerGroupProgresses) {
        final File[] groupAndSubjectDirs = consumerIdDir.listFiles();
        if (groupAndSubjectDirs != null) {
            for (final File groupAndSubjectDir : groupAndSubjectDirs) {
                if (!groupAndSubjectDir.isDirectory()) {
                    continue;
                }
                final File[] segments = groupAndSubjectDir.listFiles();
                if (segments == null || segments.length == 0) {
                    LOG.info("need delete empty pull log dir: {}", groupAndSubjectDir.getAbsolutePath());
                    continue;
                }

                final String consumerId = consumerIdDir.getName();
                final String groupAndSubject = groupAndSubjectDir.getName();

                final Long maxSequence = getPullLogMaxSequence(consumerGroupProgresses, groupAndSubject, consumerId);
                if (maxSequence == null) {
                    putPullLog(consumerId, groupAndSubject, new PullLog(config, consumerId, groupAndSubject));
                } else {
                    putPullLog(consumerId, groupAndSubject, new PullLog(config, consumerId, groupAndSubject, maxSequence));
                }
            }
        }
    }

    private void putPullLog(final String consumerId, final String groupAndSubject, final PullLog log) {
        final Lock lock = logsGuard.writeLock();
        lock.lock();
        try {
            logs.put(consumerId, groupAndSubject, log);
        } finally {
            lock.unlock();
        }
    }

    private Long getPullLogMaxSequence(final Table<String, String, ConsumerGroupProgress> consumerGroupProgresses,
                                       final String groupAndSubject, final String consumerId) {
        final GroupAndSubject parsedGroupAndSubject = GroupAndSubject.parse(groupAndSubject);
        final String subject = parsedGroupAndSubject.getSubject();
        final String group = parsedGroupAndSubject.getGroup();

        final ConsumerGroupProgress groupProgress = consumerGroupProgresses.get(subject, group);
        if (groupProgress == null) {
            return null;
        }

        final ConsumerProgress progress = groupProgress.getConsumers().get(consumerId);
        if (progress == null) {
            return null;
        } else {
            return progress.getPull();
        }
    }

    public PullLog get(final String subject, final String group, final String consumerId) {
        String groupAndSubject = groupAndSubject(subject, group);
        final Lock lock = logsGuard.readLock();
        lock.lock();
        try {
            return logs.get(consumerId, groupAndSubject);
        } finally {
            lock.unlock();
        }
    }

    public PullLog getOrCreate(final String subject, final String group, final String consumerId) {
        final String groupAndSubject = groupAndSubject(subject, group);
        final Lock readLock = logsGuard.readLock();
        readLock.lock();
        try {
            PullLog pullLog = logs.get(consumerId, groupAndSubject);
            if (pullLog != null)
                return pullLog;
        } finally {
            readLock.unlock();
        }

        final Lock writeLock = logsGuard.writeLock();
        writeLock.lock();
        try {
            if (!logs.contains(consumerId, groupAndSubject)) {
                logs.put(consumerId, groupAndSubject, new PullLog(config, consumerId, groupAndSubject));
            }

            return logs.get(consumerId, groupAndSubject);
        } finally {
            writeLock.unlock();
        }
    }

    public Table<String, String, PullLog> getLogs() {
        final Lock lock = logsGuard.readLock();
        lock.lock();
        try {
            return HashBasedTable.create(logs);
        } finally {
            lock.unlock();
        }
    }

    public void flush() {
        final long start = System.currentTimeMillis();
        List<PullLog> list = allLogs();
        try {
            for (final PullLog log : list) {
                log.flush();
            }
        } finally {
            QMon.flushPullLogTimer(System.currentTimeMillis() - start);
        }
    }

    private List<PullLog> allLogs() {
        Lock lock = logsGuard.readLock();
        lock.lock();
        try {
            return new ArrayList<>(logs.values());
        } finally {
            lock.unlock();
        }
    }

    public void clean(Collection<ConsumerGroupProgress> progresses) {
        for (ConsumerGroupProgress progress : progresses) {
            final Map<String, ConsumerProgress> consumers = progress.getConsumers();
            if (consumers == null || consumers.isEmpty()) {
                continue;
            }

            for (final ConsumerProgress consumer : consumers.values()) {
                PullLog pullLog = get(consumer.getSubject(), consumer.getGroup(), consumer.getConsumerId());
                if (pullLog == null) continue;

                pullLog.clean(consumer.getAck());
            }
        }
    }

    public boolean destroy(String subject, String group, String consumerId) {
        PullLog pullLog = get(subject, group, consumerId);
        if (pullLog == null) return false;

        pullLog.destroy();
        remove(subject, group, consumerId);
        return true;
    }

    private void remove(String subject, String group, String consumerId) {
        String groupAndSubject = groupAndSubject(subject, group);
        final Lock lock = logsGuard.writeLock();
        lock.lock();
        try {
            logs.remove(consumerId, groupAndSubject);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        for (final PullLog log : logs.values()) {
            log.close();
        }
    }
}
