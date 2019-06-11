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

package qunar.tc.qmq.backup.service;

import com.google.common.base.CharMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_FLUSH_INTERVAL;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.SYNC_OFFSET_FLUSH_INTERVAL_CONFIG_KEY;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-14 13:06
 */
public class IndexLogIterateService implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(IndexLogIterateService.class);

    private final IndexLog log;
    private final FixedExecOrderEventBus dispatcher;
    private final Thread dispatcherThread;
    private CheckpointManager checkpointManager;
    private final PeriodicFlushService flushService;

    private final LongAdder iterateFrom;
    private volatile boolean stop = false;

    public IndexLogIterateService(final DynamicConfig config, final IndexLog log, final CheckpointManager offsetManager, final FixedExecOrderEventBus dispatcher) {
        this.log = log;
        this.dispatcher = dispatcher;
        this.dispatcherThread = new Thread(new Dispatcher());
        this.dispatcherThread.setName("MessageLogIterator");
        this.iterateFrom = new LongAdder();
        this.flushService = new PeriodicFlushService(new FlushIndexIterateCheckpoint(config));
        this.checkpointManager = offsetManager;
        this.iterateFrom.add(initialMessageIterateFrom(log, offsetManager));
        Metrics.gauge("ReplayIndexLogLag", this::replayIndexLogLag);
    }

    private long initialMessageIterateFrom(final IndexLog log, final CheckpointManager checkpointManager) {
        if (checkpointManager.getIndexIterateCheckpoint() <= 0) {
            return log.minOffset();
        }
        if (checkpointManager.getIndexIterateCheckpoint() > log.maxOffset()) {
            return log.maxOffset();
        }
        return checkpointManager.getIndexIterateCheckpoint();
    }

    private Double replayIndexLogLag() {
        return (double) (log.maxOffset() - iterateFrom.longValue());
    }

    public void start() {
        this.dispatcherThread.start();
        this.flushService.start();
    }

    @Override
    public void destroy() {
        this.stop = true;
        try {
            dispatcherThread.join();
        } catch (InterruptedException e) {
            LOG.error("index log dispatcher thread interrupted", e);
        }
        flushService.close();
    }

    private class Dispatcher implements Runnable {

        @Override
        public void run() {
            while (!stop) {
                try {
                    processLog();
                } catch (Throwable e) {
                    Metrics.counter("ReplayMessageLogFailed").inc();
                    LOG.error("replay index log failed, will retry.", e);
                }
            }
        }

        private void processLog() {
            final long startOffset = iterateFrom.longValue();
            try (IndexLogVisitor visitor = log.newVisitor(startOffset)) {
                if (startOffset != visitor.getStartOffset()) {
                    LOG.info("reset iterate from offset from {} to {}", startOffset, visitor.getStartOffset());
                    iterateFrom.reset();
                    iterateFrom.add(visitor.getStartOffset());
                }

                while (true) {
                    final LogVisitorRecord<MessageQueryIndex> record = visitor.nextRecord();
                    if (record.isNoMore()) {
                        break;
                    }

                    if (record.hasData()) {
                        final MessageQueryIndex data = record.getData();
                        if (CharMatcher.INVISIBLE.matchesAnyOf(data.getSubject())) {
                            LOG.error("hit illegal subject during iterate message log, skip this message. subject: {}", data.getSubject());
                        } else {
                            data.setCurrentOffset(visitor.getStartOffset() + visitor.visitedBufferSize());
                            dispatcher.post(data);
                        }
                    }
                }
                iterateFrom.add(visitor.visitedBufferSize());

                try {
                    TimeUnit.MILLISECONDS.sleep(5);
                } catch (InterruptedException e) {
                    LOG.warn("action log dispatcher sleep interrupted");
                }
            }
        }
    }

    private class FlushIndexIterateCheckpoint implements PeriodicFlushService.FlushProvider {
        private final DynamicConfig config;

        FlushIndexIterateCheckpoint(DynamicConfig config) {
            this.config = config;
        }

        @Override
        public int getInterval() {
            return config.getInt(SYNC_OFFSET_FLUSH_INTERVAL_CONFIG_KEY, DEFAULT_FLUSH_INTERVAL);
        }

        @Override
        public void flush() {
            final long offset = iterateFrom.longValue();
            checkpointManager.saveIndexIterateCheckpointSnapshot(new Snapshot<>(offset, offset));
        }
    }
}
