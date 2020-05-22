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

package qunar.tc.qmq.backup.config;

import qunar.tc.qmq.configuration.DynamicConfig;

public class DefaultBackupConfig implements BackupConfig {
    public static final int DEFAULT_BACKUP_THREAD_SIZE = 1;
    public static final int DEFAULT_FLUSH_INTERVAL = 500;
    public static final int DEFAULT_RETRY_NUM = 5;
    public static final int DEFAULT_BATCH_SIZE = 10;
    public static final String DEFAULT_HBASE_CONFIG_FILE = "hbase.properties";
    public static final String DEFAULT_HBASE_MESSAGE_INDEX_TABLE = "qmq_backup";
    public static final String DEFAULT_HBASE_DELAY_MESSAGE_INDEX_TABLE = "qmq_backup_delay";
    public static final String DEFAULT_HBASE_RECORD_TABLE = "qmq_backup_record";
    public static final String DEFAULT_HBASE_DEAD_TABLE = "qmq_backup_dead";
    public static final String DEFAULT_HBASE_DEAD_CONTENT_TABLE = "qmq_backup_dead_content";
    public static final String DEFAULT_DB_DIC_TABLE = "qmq_dic";
    public static final String DEFAULT_DELAY_DB_DIC_TABLE = "qmq_delay_dic";
    public static final String DEFAULT_STORE_FACTORY_TYPE = "hbase";
    public static final int DEFAULT_ROCKS_DB_TTL = 7200;

    public static final String HBASE_MESSAGE_INDEX_TABLE_CONFIG_KEY = "hbase.message.table";
    public static final String HBASE_DELAY_MESSAGE_INDEX_TABLE_CONFIG_KEY = "hbase.delay.message.table";
    public static final String HBASE_RECORD_TABLE_CONFIG_KEY = "hbase.record.table";
    public static final String HBASE_DEAD_MESSAGE_CONFIG_KEY = "hbase.dead.table";
    public static final String HBASE_DEAD_MESSAGE_CONTENT_CONFIG_KEY = "hbase.dead.content.table";
    public static final String SYNC_OFFSET_FLUSH_INTERVAL_CONFIG_KEY = "sync.offset.flush.interval";
    public static final String MESSAGE_BATCH_SIZE_CONFIG_KEY = "message.backup.batch.size";
    public static final String MESSAGE_RETRY_NUM_CONFIG_KEY = "message.backup.max.retry.num";
    public static final String RECORD_BATCH_SIZE_CONFIG_KEY = "record.backup.batch.size";
    public static final String RECORD_BACKUP_RETRY_NUM_CONFIG_KEY = "record.backup.max.retry.num";
    public static final String ENABLE_RECORD_CONFIG_KEY = "enable.record";
    public static final String DEAD_MESSAGE_BACKUP_THREAD_SIZE_CONFIG_KEY = "dead.message.backup.thread.size";
    public static final String DEAD_RECORD_BACKUP_THREAD_SIZE_CONFIG_KEY = "dead.record.backup.thread.size";
    public static final String STORE_FACTORY_TYPE_CONFIG_KEY = "store.type";
    public static final String ROCKS_DB_PATH_CONFIG_KEY = "rocks.db.path";
    public static final String ROCKS_DB_TTL_CONFIG_KEY = "rocks.db.ttl";
    public static final String ACQUIRE_BACKUP_META_URL = "acquire.server.meta.url";

    private volatile String brokerGroup;

    private final DynamicConfig config;

    public DefaultBackupConfig(DynamicConfig config) {
        this.config = config;
    }

    @Override
    public String getBrokerGroup() {
        return brokerGroup;
    }

    @Override
    public void setBrokerGroup(String name) {
        this.brokerGroup = name;
    }

    @Override
    public DynamicConfig getDynamicConfig() {
        return config;
    }
}
