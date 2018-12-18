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

package qunar.tc.qmq.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.Datagram;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public class MasterSlaveSyncManager implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(MasterSlaveSyncManager.class);

    private final SlaveSyncClient slaveSyncClient;
    private final Map<SyncType, SlaveSyncTask> slaveSyncTasks;

    public MasterSlaveSyncManager(final SlaveSyncClient slaveSyncClient) {
        this.slaveSyncClient = slaveSyncClient;
        this.slaveSyncTasks = new HashMap<>();
    }

    public void registerProcessor(final SyncType syncType, final SyncLogProcessor syncLogProcessor) {
        slaveSyncTasks.put(syncType, new SlaveSyncTask(syncLogProcessor));
    }

    public void startSync() {
        for (final Map.Entry<SyncType, SlaveSyncTask> entry : slaveSyncTasks.entrySet()) {
            new Thread(entry.getValue(), "sync-task-" + entry.getKey().name()).start();
        }
    }

    @Override
    public void destroy() {
        for (SlaveSyncTask task : slaveSyncTasks.values()) {
            try {
                task.shutdown();
            } catch (Exception e) {
                LOG.error("disposable destroy failed", e);
            }
        }
    }

    private class SlaveSyncTask implements Runnable {
        private final SyncLogProcessor processor;
        private final String processorName;

        private volatile boolean running = true;

        SlaveSyncTask(SyncLogProcessor processor) {
            this.processor = processor;
            this.processorName = processor.getClass().getSimpleName();
        }

        @Override
        public void run() {
            while (running) {
                final long start = System.currentTimeMillis();
                try {
                    doSync();
                } catch (Throwable e) {
                    QMon.syncTaskSyncFailedCountInc(BrokerConfig.getBrokerName());
                    LOG.error("[{}] sync data from master failed, will retry after 2 seconds.", processorName, e);

                    silentSleep(2);
                } finally {
                    QMon.syncTaskExecTimer(processorName, System.currentTimeMillis() - start);
                }
            }
        }

        private void doSync() {
            Datagram response = null;
            try {
                response = slaveSyncClient.syncLog(processor.getRequest());
                processor.process(response);
            } finally {
                if (response != null) {
                    response.release();
                }
            }
        }

        private void silentSleep(final long timeoutInSeconds) {
            try {
                TimeUnit.SECONDS.sleep(timeoutInSeconds);
            } catch (InterruptedException ignore) {
                LOG.debug("sleep interrupted");
            }
        }

        void shutdown() {
            running = false;
        }
    }
}
