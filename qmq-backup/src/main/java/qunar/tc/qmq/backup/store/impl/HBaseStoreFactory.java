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

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_HBASE_CONFIG_FILE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_HBASE_DEAD_CONTENT_TABLE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_HBASE_DEAD_TABLE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_HBASE_DELAY_MESSAGE_INDEX_TABLE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_HBASE_MESSAGE_INDEX_TABLE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_HBASE_RECORD_TABLE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.HBASE_DEAD_MESSAGE_CONFIG_KEY;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.HBASE_DEAD_MESSAGE_CONTENT_CONFIG_KEY;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.HBASE_DELAY_MESSAGE_INDEX_TABLE_CONFIG_KEY;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.HBASE_MESSAGE_INDEX_TABLE_CONFIG_KEY;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.HBASE_RECORD_TABLE_CONFIG_KEY;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.B_FAMILY;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.B_MESSAGE_QUALIFIERS;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.B_RECORD_QUALIFIERS;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.R_FAMILY;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.store.RecordStore;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.utils.CharsetUtils;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-07 19:13
 */
public class HBaseStoreFactory implements KvStore.StoreFactory {
    private static final short CLIENT_FLUSH_INTERVAL = (short) TimeUnit.SECONDS.toMillis(1);
    private static final int CLIENT_BUFFER_SIZE = 8 * 1024;

    private final HBaseClient client;

    private final String table;
    private final String delayTable;
    private final String recordTable;
    private final String deadTable;
    private final String deadContentTable;

    private final DicService dicService;
    private final BackupKeyGenerator keyGenerator;

    HBaseStoreFactory(DynamicConfig config, DicService dicService, BackupKeyGenerator keyGenerator) {
        final DynamicConfig hbaseConfig = DynamicConfigLoader.load(DEFAULT_HBASE_CONFIG_FILE, true);
        final Config HBaseConfig = from(hbaseConfig);
        this.client = new HBaseClient(HBaseConfig);
        this.client.setFlushInterval(CLIENT_FLUSH_INTERVAL);
        this.client.setIncrementBufferSize(CLIENT_BUFFER_SIZE);
        this.table = config.getString(HBASE_MESSAGE_INDEX_TABLE_CONFIG_KEY, DEFAULT_HBASE_MESSAGE_INDEX_TABLE);
        this.delayTable = config.getString(HBASE_DELAY_MESSAGE_INDEX_TABLE_CONFIG_KEY, DEFAULT_HBASE_DELAY_MESSAGE_INDEX_TABLE);
        this.recordTable = config.getString(HBASE_RECORD_TABLE_CONFIG_KEY, DEFAULT_HBASE_RECORD_TABLE);
        this.deadTable = config.getString(HBASE_DEAD_MESSAGE_CONFIG_KEY, DEFAULT_HBASE_DEAD_TABLE);
        this.deadContentTable = config.getString(HBASE_DEAD_MESSAGE_CONTENT_CONFIG_KEY, DEFAULT_HBASE_DEAD_CONTENT_TABLE);

        this.dicService = dicService;
        this.keyGenerator = keyGenerator;
    }

    private static Config from(DynamicConfig config) {
        Map<String, String> map = config.asMap();
        final Config hbaseConfig = new Config();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            hbaseConfig.overrideConfig(entry.getKey(), entry.getValue());
        }
        return hbaseConfig;
    }

    @Override
    public MessageStore createMessageIndexStore() {
        byte[] table = CharsetUtils.toUTF8Bytes(this.table);
        return new HBaseIndexStore(table, B_FAMILY, B_MESSAGE_QUALIFIERS, client, dicService);
    }

    @Override
    public RecordStore createRecordStore() {
        byte[] table = CharsetUtils.toUTF8Bytes(recordTable);
        byte[] indexTable = CharsetUtils.toUTF8Bytes(this.table);
        return new HBaseRecordStore(table, indexTable, R_FAMILY, B_RECORD_QUALIFIERS, client, dicService, keyGenerator);
    }

    @Override
    public MessageStore createDeadMessageStore() {
        byte[] table = CharsetUtils.toUTF8Bytes(deadTable);
        return new HBaseDeadMessageStore(table, B_FAMILY, B_MESSAGE_QUALIFIERS, client, dicService);
    }

    @Override
    public MessageStore createDeadMessageContentStore() {
        byte[] table = CharsetUtils.toUTF8Bytes(deadContentTable);
        return new HBaseDeadMessageContentStore(table, B_FAMILY, B_MESSAGE_QUALIFIERS, client, dicService);
    }
}
