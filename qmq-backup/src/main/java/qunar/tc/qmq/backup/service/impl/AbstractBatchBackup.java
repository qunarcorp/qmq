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

import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.service.BatchBackup;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author yiqun.fan create on 17-9-25.
 */
public abstract class AbstractBatchBackup<T> implements BatchBackup<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchBackup.class);

    private static final long MAX_STORE_INTERVAL = TimeUnit.SECONDS.toMillis(30);

    protected static final String[] TYPE_ARRAY = new String[]{"type"};
    protected static final String[] INDEX_TYPE = new String[]{"messageIndex"};
    protected static final String[] RECORD_TYPE = new String[]{"record"};
    protected static final String[] DEAD_MESSAGE_TYPE = new String[]{"deadMessage"};
    protected static final String[] DEAD_RECORD_TYPE = new String[]{"deadRecord"};

    protected final DynamicConfig config;
    protected final byte[] brokerGroupBytes;
    protected final int brokerGroupLength;
    private final String backupName;
    private final QmqCounter storeExceptionCounter;
    private final ScheduledExecutorService forceStoreExecutor;

    private final ReentrantLock batchGuard = new ReentrantLock();
    private List<T> batch = new ArrayList<>();
    private volatile long latestStoreTime = -1;

    AbstractBatchBackup(String backupName, BackupConfig config) {
        this.config = config.getDynamicConfig();
        this.brokerGroupBytes = Bytes.UTF8(config.getBrokerGroup());
        this.brokerGroupLength = this.brokerGroupBytes.length;
        this.backupName = backupName;
        this.storeExceptionCounter = Metrics.counter(backupName + "_store_exception");
        this.forceStoreExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(backupName + "-force-store"));
    }

    @Override
    public void start() {
        forceStoreExecutor.scheduleAtFixedRate(this::tryForceStore, 10, 10, TimeUnit.SECONDS);
    }

    private void tryForceStore() {
        try {
            final long interval = System.currentTimeMillis() - latestStoreTime;
            if (interval < MAX_STORE_INTERVAL) {
                return;
            }

            List<T> batch;
            batchGuard.lock();
            try {
                batch = getMinBatchOrNull(1);
            } finally {
                batchGuard.unlock();
            }
            storeBatch(batch, null);
        } catch (Throwable e) {
            LOGGER.warn("force store batch for {} failed.", backupName, e);
        }
    }

    @Override
    public void add(T t, Consumer<T> fi) {
        storeBatch(tryCreateBatch(t), fi);
    }

    private List<T> tryCreateBatch(final T t) {
        batchGuard.lock();
        try {
            batch.add(t);
            return getMinBatchOrNull(getBatchSize());
        } finally {
            batchGuard.unlock();
        }
    }

    private List<T> getMinBatchOrNull(final int minBatchSize) {
        if (batch.size() < minBatchSize) {
            return null;
        }

        final List<T> tmp = batch;
        batch = new ArrayList<>(getBatchSize());
        return tmp;
    }

    private void storeBatch(final List<T> batch, final Consumer<T> fi) {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        try {
            store(batch, fi);
            latestStoreTime = System.currentTimeMillis();
        } catch (Exception e) {
            storeExceptionCounter.inc();
            LOGGER.error("{} store backup error", backupName, e);
        }
    }

    @Override
    public void close() {
        try {
            forceStoreExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Shutdown {} forceStoreExecutor interrupted.", backupName, e);
        }
        doStop();
    }

    protected abstract void doStop();

    protected abstract void store(List<T> batch, Consumer<T> fi);

    protected abstract int getBatchSize();
}
