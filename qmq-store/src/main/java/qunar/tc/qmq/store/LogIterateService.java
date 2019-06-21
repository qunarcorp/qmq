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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author keli.wang
 * @since 2019-06-18
 */
public class LogIterateService<T> implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LogIterateService.class);

    private final String name;
    private final StorageConfig config;
    private final Visitable<T> visitable;
    private final FixedExecOrderEventBus dispatcher;
    private final Thread dispatcherThread;

    private final LongAdder iterateFrom;
    private volatile boolean stop = false;

    LogIterateService(final String name, final StorageConfig config, final Visitable<T> visitable, final long checkpoint, final FixedExecOrderEventBus dispatcher) {
        this.name = name;
        this.config = config;
        this.visitable = visitable;
        this.dispatcher = dispatcher;
        this.dispatcherThread = new Thread(new Dispatcher());
        this.dispatcherThread.setName(name);
        this.iterateFrom = new LongAdder();
        this.iterateFrom.add(initialMessageIterateFrom(visitable, checkpoint));

        QMon.replayLag(name + "Lag", () -> (double) replayLogLag());
    }

    private long initialMessageIterateFrom(final Visitable<T> log, final long checkpoint) {
        if (checkpoint <= 0) {
            return log.getMinOffset();
        }
        if (checkpoint > log.getMaxOffset()) {
            return log.getMaxOffset();
        }
        return checkpoint;
    }

    public void start() {
        dispatcherThread.start();
    }

    public void blockUntilReplayDone() {
        LOG.info("replay log initial lag: {}; min: {}, max: {}, from: {}",
                replayLogLag(), visitable.getMinOffset(), visitable.getMaxOffset(), iterateFrom.longValue());

        while (replayLogLag() > 0) {
            LOG.info("waiting replay log ...");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOG.warn("block until replay done interrupted", e);
            }
        }
    }

    private long replayLogLag() {
        return visitable.getMaxOffset() - iterateFrom.longValue();
    }

    @Override
    public void close() {
        stop = true;
        try {
            dispatcherThread.join();
        } catch (InterruptedException e) {
            LOG.error("log dispatcher thread interrupted", e);
        }
    }

    private class Dispatcher implements Runnable {
        @Override
        public void run() {
            while (!stop) {
                try {
                    processLog();
                } catch (Throwable e) {
                    QMon.replayLogFailedCountInc(name + "Failed");
                    LOG.error("replay log failed, will retry.", e);
                }
            }
        }

        private void processLog() {
            final long startOffset = iterateFrom.longValue();
            try (AbstractLogVisitor<T> visitor = visitable.newVisitor(startOffset)) {
                if (startOffset != visitor.getStartOffset()) {
                    LOG.info("reset iterate from offset from {} to {}", startOffset, visitor.getStartOffset());
                    iterateFrom.reset();
                    iterateFrom.add(visitor.getStartOffset());
                }

                while (true) {
                    final LogVisitorRecord<T> record = visitor.nextRecord();
                    if (record.isNoMore()) {
                        break;
                    }

                    if (record.hasData()) {
                        dispatcher.post(record.getData());
                    }
                }
                iterateFrom.add(visitor.visitedBufferSize());
            }

            try {
                TimeUnit.MILLISECONDS.sleep(config.getLogDispatcherPauseMillis());
            } catch (InterruptedException e) {
                LOG.warn("log dispatcher sleep interrupted");
            }
        }
    }
}