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

package qunar.tc.qmq.backup.store.impl;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.store.RocksDBStore;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.utils.CharsetUtils;

import java.io.File;
import java.util.Optional;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.*;

/**
 * @author yunfeng.yang
 * @since 2018/3/21
 */
public class RocksDBStoreImpl implements RocksDBStore {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStoreImpl.class);

    static {
        RocksDB.loadLibrary();
    }

    private final TtlDB rocksDB;

    public RocksDBStoreImpl(final DynamicConfig config) {
        final String path = config.getString(ROCKS_DB_PATH_CONFIG_KEY);
        final int ttl = config.getInt(ROCKS_DB_TTL_CONFIG_KEY, DEFAULT_ROCKS_DB_TTL);

        File file = new File(path);
        if (!file.exists() || !file.isDirectory()) {
            if (!file.mkdirs()) {
                throw new RuntimeException("Failed to create RocksDB dir.");
            }
        }

        try {
            final Options options = new Options();
            options.setCreateIfMissing(true);
            this.rocksDB = TtlDB.open(options, path, ttl, false);
            LOG.info("open rocks db success, path:{}, ttl:{}", path, ttl);
        } catch (Exception e) {
            LOG.error("open rocks db error, path:{}, ttl:{}", path, ttl, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String key, String value) {
        try {
            final byte[] keyBytes = CharsetUtils.toUTF8Bytes(key);
            final byte[] valueBytes = CharsetUtils.toUTF8Bytes(value);
            if (keyBytes == null || keyBytes.length == 0 || valueBytes == null || value.length() == 0) {
                return;
            }
            rocksDB.put(keyBytes, valueBytes);
        } catch (Exception e) {
            LOG.error("put rocks db error, key:{}, value:{}", key, value, e);
        }
    }

    @Override
    public Optional<String> get(String key) {
        try {
            final byte[] keyBytes = CharsetUtils.toUTF8Bytes(key);
            if (keyBytes == null || keyBytes.length == 0) {
                return Optional.empty();
            }
            final byte[] valueBytes = rocksDB.get(keyBytes);
            final String value = CharsetUtils.toUTF8String(valueBytes);
            if (value.length() == 0) {
                return Optional.empty();
            }
            return Optional.of(value);
        } catch (Exception e) {
            LOG.error("get value from rocks db error, key:{}", key, e);
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        rocksDB.close();
    }
}
