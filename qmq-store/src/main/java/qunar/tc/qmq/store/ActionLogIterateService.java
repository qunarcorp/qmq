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
import qunar.tc.qmq.store.action.ActionEvent;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class ActionLogIterateService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ActionLogIterateService.class);

    private final ActionLog log;
    private final FixedExecOrderEventBus dispatcher;
    private final Thread dispatcherThread;

    private final LongAdder iterateFrom;
    private volatile boolean stop = false;

    public ActionLogIterateService(final ActionLog log, final CheckpointManager checkpointManager, final FixedExecOrderEventBus dispatcher) {
        this.log = log;
        this.dispatcher = dispatcher;
        this.dispatcherThread = new Thread(new Dispatcher());
        this.dispatcherThread.setName("ActionLogIterator");
        this.iterateFrom = new LongAdder();
        this.iterateFrom.add(initialIterateFrom(log, checkpointManager));

        QMon.replayActionLogLag(() -> (double) replayActionLogLag());
    }

    private long initialIterateFrom(final ActionLog log, final CheckpointManager checkpointManager) {
        final long checkpointOffset = checkpointManager.getActionCheckpointOffset();
        final long maxOffset = log.getMaxOffset();

        if (checkpointOffset <= 0) {
            return maxOffset;
        }
        if (checkpointOffset > maxOffset) {
            return maxOffset;
        }

        return checkpointOffset;
    }

    public void start() {
        dispatcherThread.start();
    }

    public void blockUntilReplayDone() {
        LOG.info("replay action log initial lag: {}; min: {}, max: {}, from: {}",
                replayActionLogLag(), log.getMinOffset(), log.getMaxOffset(), iterateFrom.longValue());

        while (replayActionLogLag() > 0) {
            LOG.info("waiting replay action log ...");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOG.warn("block until replay done interrupted", e);
            }
        }
    }

    private long replayActionLogLag() {
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
                    QMon.replayActionLogFailedCountInc();
                    LOG.error("replay action log failed, will retry.", e);
                }
            }
        }

        private void processLog() {
            long startOffset = iterateFrom.longValue();
            final ActionLogVisitor visitor = log.newVisitor(iterateFrom.longValue());
            if (startOffset != visitor.getStartOffset()) {
                iterateFrom.reset();
                iterateFrom.add(visitor.getStartOffset());
            }
            while (true) {
                final Optional<Action> action = visitor.nextAction();
                if (action == null) {
                    break;
                }

                action.ifPresent(act -> dispatcher.post(new ActionEvent(iterateFrom.longValue() + visitor.visitedBufferSize(), act)));
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
