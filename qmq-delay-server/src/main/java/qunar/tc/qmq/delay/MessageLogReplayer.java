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

package qunar.tc.qmq.delay;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.visitor.DelayMessageLogVisitor;
import qunar.tc.qmq.delay.store.visitor.LogVisitor;
import qunar.tc.qmq.metrics.Metrics;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 17:43
 */
public class MessageLogReplayer implements Switchable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageLogReplayer.class);

    private static final RateLimiter LOG_LIMITER = RateLimiter.create((double) 1 / 6);

    private final DelayLogFacade facade;
    private final Thread dispatcherThread;
    private final LongAdder iterateFrom;
    private final EventListener<LogRecord> dispatcher;
    private volatile boolean stop = true;

    MessageLogReplayer(final DelayLogFacade facade, final Function<ScheduleIndex, Boolean> func) {
        this.facade = facade;
        this.dispatcher = new MessageIterateEventListener(facade, func);
        this.iterateFrom = new LongAdder();
        this.iterateFrom.add(facade.initialMessageIterateFrom());
        this.dispatcherThread = new Thread(new Dispatcher(iterateFrom.longValue()));

        Metrics.gauge("replayMessageLogLag", () -> (double) replayMessageLogLag());
    }

    private long replayMessageLogLag() {
        return facade.getMessageLogMaxOffset() - iterateFrom.longValue();
    }

    @Override
    public void start() {
        stop = false;
        dispatcherThread.start();
    }

    @Override
    public void shutdown() {
        stop = true;
        try {
            dispatcherThread.join();
        } catch (InterruptedException e) {
            LOGGER.error("message log replay error,iterate form:{}", iterateFrom.longValue(), e);
        }
    }

    void blockUntilReplayDone() {
        LOGGER.info("replay message log initial lag: {}; min: {}, max: {}, from: {}",
                replayMessageLogLag(), facade.getMessageLogMinOffset(), facade.getMessageLogMaxOffset(), iterateFrom.longValue());

        while (replayMessageLogLag() > 0) {
            LOGGER.info("waiting replay message log ...");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOGGER.warn("block until replay done interrupted", e);
            }
        }
    }

    private class Dispatcher implements Runnable {
        private final AtomicLong cursor;

        Dispatcher(long iterate) {
            this.cursor = new AtomicLong(iterate);
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    processLog(cursor.get());
                } catch (Throwable e) {
                    Metrics.counter("replayMessageLogFailed").inc();
                    if (LOG_LIMITER.tryAcquire()) {
                        LOGGER.error("replay message log failed, will retry.cursor:{} iterateOffset:{}", cursor.get(), iterateFrom.longValue(), e);
                    }
                }
            }
        }

        private void processLog(long cursor) {
            long iterate = iterateFrom.longValue();
            if (cursor < iterate) {
                if (LOG_LIMITER.tryAcquire()) {
                    LOGGER.info("replay message log failed,cursor < iterate,cursor:{},iterate:{}", cursor, iterate);
                }
                this.cursor.set(iterate);
            }

            if (cursor > iterate) {
                LOGGER.error("replay message log happened accident,cursor:{},iterate:{}", cursor, iterate);
                iterateFrom.add(cursor - iterate);
            }

            final LogVisitor<LogRecord> visitor = facade.newMessageLogVisitor(iterateFrom.longValue());
            adjustOffset(visitor);

            while (true) {
                final Optional<LogRecord> recordOptional = visitor.nextRecord();
                if (recordOptional.isPresent() && recordOptional.get() == DelayMessageLogVisitor.EMPTY_LOG_RECORD) {
                    break;
                }

                recordOptional.ifPresent((record) -> {
                    dispatcher.post(record);
                    long checkpoint = record.getStartWroteOffset() + record.getRecordSize();
                    this.cursor.addAndGet(record.getRecordSize());
                    facade.updateIterateOffset(checkpoint);
                });
            }
            iterateFrom.add(visitor.visitedBufferSize());

            try {
                TimeUnit.MILLISECONDS.sleep(5);
            } catch (InterruptedException e) {
                LOGGER.warn("message log iterate sleep interrupted");
            }
        }

        private void adjustOffset(final LogVisitor<LogRecord> visitor) {
            long startOffset = ((DelayMessageLogVisitor) visitor).startOffset();
            long iterateLongValue = iterateFrom.longValue();
            if (startOffset > iterateLongValue) {
                iterateFrom.add(startOffset - iterateLongValue);
                this.cursor.set(startOffset);
            }
        }
    }
}
