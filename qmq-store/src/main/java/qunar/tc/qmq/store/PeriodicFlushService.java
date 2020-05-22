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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2017/7/7
 */
public class PeriodicFlushService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicFlushService.class);

    private final String name;
    private final FlushProvider flushProvider;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> future;

    public PeriodicFlushService(final FlushProvider flushProvider) {
        this.name = flushProvider.getClass().getSimpleName();
        this.flushProvider = flushProvider;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(name));
    }

    public void start() {
        future = scheduler.scheduleWithFixedDelay(
                new FlushRunnable(),
                flushProvider.getInterval(),
                flushProvider.getInterval(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        try {
            if (future != null) {
                future.cancel(false);
            }
            LOG.info("will flush one more time for {} before shutdown flush service.", name);
            flushProvider.flush();
        } catch (Exception e) {
            LOG.error("shutdown flush service for {} failed.", name, e);
        }
    }

    public interface FlushProvider {
        int getInterval();

        void flush();
    }

    private class FlushRunnable implements Runnable {
        @Override
        public void run() {
            try {
                flushProvider.flush();
            } catch (Throwable e) {
                LOG.error("flushProvider {} flush failed.", name, e);
            }
        }
    }
}
