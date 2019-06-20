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

package qunar.tc.qmq.backup.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.service.FileStore;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.IndexLog;
import qunar.tc.qmq.store.PeriodicFlushService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_FLUSH_INTERVAL;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.SYNC_OFFSET_FLUSH_INTERVAL_CONFIG_KEY;

public class IndexFileStore implements FileStore {
    private static final Logger LOG = LoggerFactory.getLogger(IndexFileStore.class);

    private final PeriodicFlushService flushService;
    private final ScheduledExecutorService logCleanerScheduler;
    private final DynamicConfig config;
    private final IndexLog log;

    public IndexFileStore(IndexLog log, DynamicConfig config) {
        this.log = log;
        this.config = config;
        this.flushService = new PeriodicFlushService(new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return config.getInt(SYNC_OFFSET_FLUSH_INTERVAL_CONFIG_KEY, DEFAULT_FLUSH_INTERVAL);
            }

            @Override
            public void flush() {
                IndexFileStore.this.flush();
            }
        });
        this.logCleanerScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("index-log-cleaner"));
        logCleanerScheduler.scheduleAtFixedRate(this::clean, 10, 30, TimeUnit.SECONDS);
    }

    private void clean() {
        if (!config.getBoolean("enable.index.log.delete", true)) return;
        log.clean();
    }

    @Override
    public void scheduleFlush() {
        flushService.start();
    }

    @Override
    public void flush() {
        log.flush();
    }

    @Override
    public void destroy() {
        this.flushService.close();
        try {
            logCleanerScheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Shutdown log cleaner scheduler interrupted.", e);
        }
    }
}
