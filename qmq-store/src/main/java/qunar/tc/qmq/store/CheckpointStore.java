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

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author keli.wang
 * @since 2017/7/7
 */
public class CheckpointStore<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointStore.class);

    private final String storePath;
    private final String filename;
    private final Serde<T> serde;

    public CheckpointStore(final String storePath, final String filename, final Serde<T> serde) {
        this.storePath = storePath;
        this.filename = filename;
        this.serde = serde;

        ensureDir(storePath);
    }

    private void ensureDir(final String storePath) {
        final File store = new File(storePath);
        if (store.exists()) {
            return;
        }

        final boolean success = store.mkdirs();
        if (!success) {
            throw new RuntimeException("Failed create path " + storePath);
        }
        LOG.info("Create checkpoint store {} success.", storePath);
    }

    public T loadCheckpoint() {
        final File checkpointFile = checkpointFile();
        final File backupFile = backupFile();

        if (!checkpointFile.exists() && !backupFile.exists()) {
            LOG.warn("Checkpoint file and backup file does not exist, return null for now");
            return null;
        }

        try {
            final byte[] data = Files.toByteArray(checkpointFile);
            if (data != null && data.length == 0) {
                return null;
            }
            return serde.fromBytes(data);
        } catch (IOException e) {
            LOG.error("Load checkpoint file failed. Try load backup checkpoint file instead.", e);
        }

        try {
            return serde.fromBytes(Files.toByteArray(backupFile));
        } catch (IOException e) {
            LOG.error("Load backup checkpoint file failed.", e);
        }

        throw new RuntimeException("Load checkpoint failed. filename=" + filename);
    }

    // TODO(keli.wang): 根据数据量大小看看后面是不是需要进行压缩，毕竟数据量大了之后一直保存可能很耗IO
    public void saveCheckpoint(final T checkpoint) {
        final byte[] data = serde.toBytes(checkpoint);
        Preconditions.checkState(data != null, "Serialized checkpoint data should not be null.");
        if (data.length == 0) {
            return;
        }

        final File tmp = tmpFile();
        try {
            Files.write(data, tmp);
        } catch (IOException e) {
            LOG.error("write data into tmp checkpoint file failed. file={}", tmp, e);
            throw new RuntimeException("write checkpoint data failed.", e);
        }

        final File checkpointFile = checkpointFile();
        if (checkpointFile.exists()) {
            final File backupFile = backupFile();

            if (backupFile.exists() && !backupFile.delete()) {
                LOG.error("Delete backup file failed.backup: {}", backupFile);
            }

            if (!checkpointFile.renameTo(backupFile)) {
                LOG.error("Backup current checkpoint file failed. checkpoint:{}, backup: {}", checkpointFile, backupFile);
                throw new RuntimeException("Backup current checkpoint file failed");
            }

            if (checkpointFile.exists() && !checkpointFile.delete()) {
                LOG.error("Delete checkpoint file failed.backup: {}", backupFile);
            }
        }

        if (!tmp.renameTo(checkpointFile)) {
            LOG.error("Move tmp as checkpoint file failed. tmp:{}, checkpoint: {}", tmp, checkpointFile);
            throw new RuntimeException("Move tmp as checkpoint file failed.");
        }
    }

    private File checkpointFile() {
        return new File(storePath, filename);
    }

    private File tmpFile() {
        return new File(storePath, filename + ".tmp");
    }

    private File backupFile() {
        return new File(storePath, filename + ".backup");
    }

}
