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
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.action.ActionEvent;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author keli.wang
 * @since 2018/9/12
 */
public class PullLogFlusher implements FixedExecOrderEventBus.Listener<ActionEvent>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PullLogFlusher.class);

    private final StorageConfig config;
    private final CheckpointManager checkpointManager;
    private final PullLogManager pullLogManager;
    private final ScheduledExecutorService flushExecutor;
    private final AtomicLong counter;
    private volatile long latestFlushTime;

    public PullLogFlusher(final StorageConfig config, final CheckpointManager checkpointManager, final PullLogManager pullLogManager) {
        this.config = config;
        this.checkpointManager = checkpointManager;
        this.pullLogManager = pullLogManager;
        this.flushExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("pull-log-flusher"));
        this.counter = new AtomicLong(0);
        this.latestFlushTime = -1;

        scheduleForceFlushTask();
    }

    private void scheduleForceFlushTask() {
        flushExecutor.scheduleWithFixedDelay(this::tryForceSubmitFlushTask, 1, 1, TimeUnit.MINUTES);
    }

    private void tryForceSubmitFlushTask() {
        final long interval = System.currentTimeMillis() - latestFlushTime;
        if (interval < TimeUnit.MINUTES.toMillis(1)) {
            return;
        }

        submitFlushTask();
    }

    @Override
    public void onEvent(ActionEvent event) {
        final long count = counter.incrementAndGet();
        if (count < config.getActionCheckpointInterval()) {
            return;
        }

        QMon.pullLogFlusherExceedCheckpointIntervalCountInc();
        submitFlushTask();
    }

    private synchronized void submitFlushTask() {
        counter.set(0);
        latestFlushTime = System.currentTimeMillis();

        final Snapshot<ActionCheckpoint> snapshot = checkpointManager.createActionCheckpointSnapshot();
        flushExecutor.submit(() -> {
            final long start = System.currentTimeMillis();
            try {
                pullLogManager.flush();
                checkpointManager.saveActionCheckpointSnapshot(snapshot);
            } catch (Exception e) {
                QMon.pullLogFlusherFlushFailedCountInc();
                LOG.error("flush pull log failed.", e);
            } finally {
                QMon.pullLogFlusherElapsedPerExecute(System.currentTimeMillis() - start);
            }
        });
    }

    @Override
    public void close() {
        LOG.info("try flush one more time before exit.");
        submitFlushTask();
        flushExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            LOG.warn("interrupted during closing pull log flusher.");
        }
    }
}
