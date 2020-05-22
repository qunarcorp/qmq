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

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.concurrent.NamedThreadFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2018/9/10
 */
public class SnapshotStore<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotStore.class);

    private final String prefix;
    private final StorageConfig config;
    private final File storePath;
    private final Serde<T> serde;
    private final ConcurrentSkipListMap<Long, Snapshot<T>> snapshots;
    private final ScheduledExecutorService cleanerExecutor;

    public SnapshotStore(final String name, final StorageConfig config, final Serde<T> serde) {
        this.prefix = name + ".";
        this.config = config;
        this.storePath = new File(config.getCheckpointStorePath());
        this.serde = serde;
        this.snapshots = new ConcurrentSkipListMap<>();
        this.cleanerExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(name + "-snapshot-cleaner"));

        ensureStorePath();
        loadAllSnapshots();
        scheduleSnapshotCleaner();
    }

    private void ensureStorePath() {
        if (storePath.exists()) {
            return;
        }

        final boolean success = storePath.mkdirs();
        if (!success) {
            throw new RuntimeException("failed create directory " + storePath);
        }
        LOG.info("create snapshot store directory {} success.", storePath);
    }

    private void loadAllSnapshots() {
        for (final File file : scanAllSnapshotFiles()) {
            loadSnapshotFile(file);
        }
    }

    private File[] scanAllSnapshotFiles() {
        final File[] files = storePath.listFiles((dir, name) -> name.startsWith(prefix));
        if (files == null) {
            return new File[0];
        } else {
            return files;
        }
    }

    private void loadSnapshotFile(final File file) {
        try {
            final long version = parseSnapshotVersion(file.getName());
            final byte[] data = Files.toByteArray(file);
            final Snapshot<T> snapshot = new Snapshot<>(version, serde.fromBytes(data));
            snapshots.put(version, snapshot);
            LOG.info("load snapshot file {} success.", file.getAbsolutePath());
        } catch (Exception e) {
            LOG.error("load snapshot file {} failed.", file.getAbsolutePath(), e);
        }
    }

    private long parseSnapshotVersion(final String filename) {
        final int beginIndex = prefix.length();
        return Long.parseLong(filename.substring(beginIndex));
    }

    private void scheduleSnapshotCleaner() {
        cleanerExecutor.scheduleAtFixedRate(this::tryCleanExpiredSnapshot, 1, 1, TimeUnit.MINUTES);
    }

    private void tryCleanExpiredSnapshot() {
        try {
            while (true) {
                if (snapshots.size() <= config.getCheckpointRetainCount()) {
                    return;
                }

                removeOldestSnapshot();
            }
        } catch (Throwable e) {
            LOG.error("try clean expired snapshot file failed.", e);
        }
    }

    private void removeOldestSnapshot() {
        final Map.Entry<Long, Snapshot<T>> firstEntry = snapshots.pollFirstEntry();
        final File snapshotFile = getSnapshotFile(firstEntry.getValue());
        final boolean success = snapshotFile.delete();
        if (success) {
            LOG.debug("delete snapshot file {} success.", snapshotFile.getAbsolutePath());
        } else {
            LOG.warn("delete snapshot file {} failed.", snapshotFile.getAbsolutePath());
        }
    }

    public Snapshot<T> latestSnapshot() {
        final Map.Entry<Long, Snapshot<T>> entry = snapshots.lastEntry();
        return entry == null ? null : entry.getValue();
    }

    public synchronized void saveSnapshot(final Snapshot<T> snapshot) {
        if (snapshots.containsKey(snapshot.getVersion())) {
            return;
        }

        final File tmpFile = tmpFile();
        try {
            Files.write(serde.toBytes(snapshot.getData()), tmpFile);
        } catch (IOException e) {
            LOG.error("write data into tmp snapshot file failed. file: {}", tmpFile, e);
            throw new RuntimeException("write snapshot data failed.", e);
        }

        final File snapshotFile = getSnapshotFile(snapshot);
        if (!tmpFile.renameTo(snapshotFile)) {
            LOG.error("Move tmp as snapshot file failed. tmp: {}, snapshot: {}", tmpFile, snapshotFile);
            throw new RuntimeException("Move tmp as snapshot file failed.");
        }

        snapshots.put(snapshot.getVersion(), snapshot);
    }

    private File tmpFile() {
        final String uuid = UUID.randomUUID().toString();
        return new File(storePath, uuid + ".tmp");
    }

    private File getSnapshotFile(final Snapshot<T> snapshot) {
        final String filename = prefix + StoreUtils.offset2FileName(snapshot.getVersion());
        return new File(storePath, filename);
    }

    public void close() {
        cleanerExecutor.shutdown();
        try {
            cleanerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            LOG.warn("interrupted during shutdown cleaner executor with prefix {}", prefix);
        }
    }
}
