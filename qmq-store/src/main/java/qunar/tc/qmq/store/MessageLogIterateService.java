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
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public class MessageLogIterateService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageLogIterateService.class);

    private final MessageLog log;
    private final FixedExecOrderEventBus dispatcher;
    private final Thread dispatcherThread;

    private final LongAdder iterateFrom;
    private volatile boolean stop = false;

    public MessageLogIterateService(final MessageLog log, final CheckpointManager checkpointManager, final FixedExecOrderEventBus dispatcher) {
        this.log = log;
        this.dispatcher = dispatcher;
        this.dispatcherThread = new Thread(new Dispatcher());
        this.dispatcherThread.setName("MessageLogIterator");
        this.iterateFrom = new LongAdder();
        this.iterateFrom.add(initialMessageIterateFrom(log, checkpointManager));

        QMon.replayMessageLogLag(() -> (double) replayMessageLogLag());
    }

    private long initialMessageIterateFrom(final MessageLog log, final CheckpointManager checkpointManager) {
        if (checkpointManager.getMessageCheckpointOffset() <= 0) {
            return log.getMaxOffset();
        }
        if (checkpointManager.getMessageCheckpointOffset() > log.getMaxOffset()) {
            return log.getMaxOffset();
        }
        return checkpointManager.getMessageCheckpointOffset();
    }

    public void start() {
        dispatcherThread.start();
    }

    public void blockUntilReplayDone() {
        LOG.info("replay message log initial lag: {}; min: {}, max: {}, from: {}",
                replayMessageLogLag(), log.getMinOffset(), log.getMaxOffset(), iterateFrom.longValue());

        while (replayMessageLogLag() > 0) {
            LOG.info("waiting replay message log ...");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOG.warn("block until replay done interrupted", e);
            }
        }
    }

    private long replayMessageLogLag() {
        return log.getMaxOffset() - iterateFrom.longValue();
    }

    @Override
    public void close() {
        stop = true;
        try {
            dispatcherThread.join();
        } catch (InterruptedException e) {
            LOG.error("action log dispatcher thread interrupted", e);
        }
    }

    private class Dispatcher implements Runnable {
        @Override
        public void run() {
            while (!stop) {
                try {
                    processLog();
                } catch (Throwable e) {
                    QMon.replayMessageLogFailedCountInc();
                    LOG.error("replay message log failed, will retry.", e);
                }
            }
        }

        private void processLog() {
            final long startOffset = iterateFrom.longValue();
            final MessageLogMetaVisitor visitor = log.newVisitor(startOffset);
            if (startOffset != visitor.getStartOffset()) {
                iterateFrom.reset();
                iterateFrom.add(visitor.getStartOffset());
            }

            while (true) {
                final Optional<MessageLogMeta> meta = visitor.nextRecord();
                if (meta == null) {
                    break;
                }

                meta.ifPresent(dispatcher::post);
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
