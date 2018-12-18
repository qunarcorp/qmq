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

package qunar.tc.qmq.configuration.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.concurrent.NamedThreadFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2018-11-27
 */
class ConfigWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigWatcher.class);

    private final CopyOnWriteArrayList<Watch> watches;
    private final ScheduledExecutorService watcherExecutor;

    ConfigWatcher() {
        this.watches = new CopyOnWriteArrayList<>();
        this.watcherExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("local-config-watcher"));

        start();
    }

    private void start() {
        watcherExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                checkAllWatches();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void checkAllWatches() {
        for (Watch watch : watches) {
            try {
                checkWatch(watch);
            } catch (Exception e) {
                LOG.error("check config failed. config: {}", watch.getConfig(), e);
            }
        }
    }

    private void checkWatch(final Watch watch) {
        final LocalDynamicConfig config = watch.getConfig();
        final long lastModified = config.getLastModified();
        if (lastModified == watch.getLastModified()) {
            return;
        }

        watch.setLastModified(lastModified);
        config.onConfigModified();
    }

    void addWatch(final LocalDynamicConfig config) {
        final Watch watch = new Watch(config);
        watch.setLastModified(config.getLastModified());
        watches.add(watch);
    }

    private static final class Watch {
        private final LocalDynamicConfig config;
        private volatile long lastModified;

        private Watch(final LocalDynamicConfig config) {
            this.config = config;
        }

        public LocalDynamicConfig getConfig() {
            return config;
        }

        long getLastModified() {
            return lastModified;
        }

        void setLastModified(final long lastModified) {
            this.lastModified = lastModified;
        }
    }
}
