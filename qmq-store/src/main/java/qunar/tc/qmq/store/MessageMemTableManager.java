/*
 * Copyright 2019 Qunar, Inc.
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

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * All memtables need add to current-active deque at first.
 * When a memtable is completed, add a new memtable to current-active deque and
 * add competed memtable to pending-evicted queue.
 * At the same time, there is a background thread keep polling completed memtable from
 * pending-evicted queue and use {@link MemTableEvictedCallback} to handle it.
 * After callback complete successfully, remove it from current-active deque.
 * Keep retry if callback failed.
 * <p>
 * When get message from memtables, try get from newest memtable to oldest table in current-active deque.
 *
 * @author keli.wang
 * @since 2019-06-10
 */
public class MessageMemTableManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageMemTableManager.class);

    private final StorageConfig config;
    private final int tableCapacity;
    private final AtomicInteger notEvictedCount;
    private final LinkedBlockingQueue<MessageMemTable> pendingEvicted;
    private final LinkedBlockingDeque<MessageMemTable> currentActive;

    private final ConcurrentMap<String, Long> maxMessageSequences;

    private final MemTableEvictedCallback evictedCallback;
    private final Thread evictThread;
    private volatile boolean running;

    public MessageMemTableManager(final StorageConfig config, final int tableCapacity, final MemTableEvictedCallback evictedCallback) {
        this.config = config;
        this.tableCapacity = tableCapacity;
        this.notEvictedCount = new AtomicInteger(0);
        this.pendingEvicted = new LinkedBlockingQueue<>();
        this.currentActive = new LinkedBlockingDeque<>();

        this.maxMessageSequences = new ConcurrentHashMap<>();

        this.evictedCallback = evictedCallback;
        this.evictThread = new Thread(this::evictLoop);
        this.evictThread.setName("evict-memtable-thread");
        this.running = true;
        this.evictThread.start();
    }

    private void evictLoop() {
        while (running) {
            try {
                evict();
            } catch (Throwable e) {
                LOG.error("evict failed.", e);
            }
        }
    }

    private void evict() {
        pollPendingEvictedTable().ifPresent(table -> {
            while (true) {
                try {
                    final Stopwatch stopwatch = Stopwatch.createStarted();
                    if (!evictedCallback.onEvicted(table)) {
                        LOG.error("evict memtable failed, will retry. table: {}", table);
                        continue;
                    }
                    stopwatch.stop();
                    final long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                    LOG.info("evicted memtable success. table: {}, elapsed: {}ms", table, elapsed);

                    while (currentActive.size() > config.getMaxReservedMemTable()) {
                        final MessageMemTable active = currentActive.peekFirst();
                        if (active == null) {
                            break;
                        }
                        if (active.getTabletId() > table.getTabletId()) {
                            break;
                        }

                        currentActive.pollFirst();
                        active.close();
                    }
                    notEvictedCount.decrementAndGet();
                    return;
                } catch (Throwable e) {
                    LOG.error("evict memtable failed, will retry. table: {}", table, e);
                }
            }
        });
    }

    private Optional<MessageMemTable> pollPendingEvictedTable() {
        try {
            final MessageMemTable table = pendingEvicted.poll(1, TimeUnit.SECONDS);
            return Optional.ofNullable(table);
        } catch (InterruptedException e) {
            LOG.warn("poll pending evicted memtable interrupted.", e);
            return Optional.empty();
        }
    }

    public boolean hasPendingEvicted() {
        return notEvictedCount.get() > 0;
    }

    public int getActiveCount() {
        return currentActive.size();
    }

    public MessageMemTable latestMemTable() {
        return currentActive.peekFirst();
    }

    public Iterator<MessageMemTable> iterator() {
        return currentActive.descendingIterator();
    }

    public MessageMemTable rollingNewMemTable(final long tabletId, final long beginOffset) {
        final MessageMemTable lastActiveTable = currentActive.peekLast();
        if (lastActiveTable != null) {
            notEvictedCount.incrementAndGet();
            pendingEvicted.add(lastActiveTable);
        }

        final MessageMemTable table = new MessageMemTable(tabletId, beginOffset, tableCapacity);
        currentActive.addLast(table);
        return table;
    }

    public void updateMaxMessageSequence(final String subject, final long maxMessageSequence) {
        maxMessageSequences.merge(subject, maxMessageSequence, Math::max);
    }

    public Long getMaxMessageSequence(final String subject) {
        return maxMessageSequences.get(subject);
    }

    @Override
    public void close() throws Exception {
        running = false;
        evictThread.join();
    }

    public interface MemTableEvictedCallback {
        boolean onEvicted(final MessageMemTable table);
    }
}