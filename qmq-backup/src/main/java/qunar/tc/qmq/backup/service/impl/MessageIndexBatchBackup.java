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
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.*;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-06 11:02
 */
public class MessageIndexBatchBackup extends AbstractBatchBackup<MessageQueryIndex> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageIndexBatchBackup.class);

    private final KvStore indexStore;
    private final BackupKeyGenerator keyGenerator;
    private final String brokerGroup;
    private final DynamicConfig config;

    public MessageIndexBatchBackup(BackupConfig config, KvStore indexStore, BackupKeyGenerator keyGenerator) {
        super("messageIndexBackup", config);
        this.indexStore = indexStore;
        this.keyGenerator = keyGenerator;
        this.brokerGroup = config.getBrokerGroup();
        this.config = config.getDynamicConfig();
    }

    private void saveIndex(List<MessageQueryIndex> indices, Consumer<MessageQueryIndex> fi) {
        int size = indices.size();
        byte[][] keys = new byte[size][];
        byte[][][] values = new byte[size][][];
        MessageQueryIndex tailIndex = null;
        for (int i = 0; i < size; ++i) {
            MessageQueryIndex index = indices.get(i);
            String subject = index.getSubject();
            monitorBackupIndexQps(subject);
            try {
                String realSubject = RetryPartitionUtils.getRealPartitionName(subject);
                String subjectKey = realSubject;
                String consumerGroup = null;
                if (RetryPartitionUtils.isRetryPartitionName(subject)) {
                    subjectKey = RetryPartitionUtils.buildRetryPartitionName(realSubject);
                    consumerGroup = RetryPartitionUtils.getConsumerGroup(subject);
                }
                final byte[] key = keyGenerator.generateMessageKey(subjectKey, new Date(index.getCreateTime()), index.getMessageId(), brokerGroup, consumerGroup, index.getSequence());
                final String messageId = index.getMessageId();
                final byte[] messageIdBytes = Bytes.UTF8(messageId);

                final byte[] value = new byte[20 + brokerGroupLength + messageIdBytes.length];
                Bytes.setLong(value, index.getSequence(), 0);
                Bytes.setLong(value, index.getCreateTime(), 8);
                Bytes.setInt(value, brokerGroupLength, 16);
                System.arraycopy(brokerGroupBytes, 0, value, 20, brokerGroupLength);
                System.arraycopy(messageIdBytes, 0, value, 20 + brokerGroupLength, messageIdBytes.length);

                keys[i] = key;
                values[i] = new byte[][]{value};

                if (tailIndex == null || tailIndex.compareTo(index) < 0) tailIndex = index;
            } catch (Exception e) {
                LOGGER.error("batch backup message index failed.", e);
                monitorStoreIndexError(subject);
                // tailIndex ? what ever,skipped is okay.
            }
        }
        indexStore.batchSave(keys, values);
        if (fi != null) fi.accept(tailIndex);
    }

    private void retry(MessageQueryIndex failMessage, Consumer<MessageQueryIndex> fi) {
        final int tryStoreNum = failMessage.getBackupRetryTimes();
        if (tryStoreNum < retryNum()) {
            monitorStoreRetry(failMessage.getSubject());
            failMessage.setBackupRetryTimes(tryStoreNum + 1);
            add(failMessage, fi);
        } else {
            monitorStoreDiscard(failMessage.getSubject());
            LOGGER.warn("message_index backup store discard. subject={}, messageId={}", failMessage.getSubject(), failMessage.getMessageId());
        }
    }

    private static void monitorStoreRetry(String subject) {
        Metrics.counter("message_index_backup_store_retry", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private static void monitorStoreIndexError(String subject) {
        Metrics.counter("message_index_store_error", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private static void monitorStoreDiscard(String subject) {
        Metrics.counter("message_index_backup_store_discard", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private int retryNum() {
        return config.getInt(MESSAGE_RETRY_NUM_CONFIG_KEY, DEFAULT_RETRY_NUM);
    }

    @Override
    protected void doStop() {
        try {
            if (indexStore != null) indexStore.close();
        } catch (Exception e) {
            LOGGER.error("close {} failed.", indexStore);
        }
    }

    @Override
    protected void store(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> fi) {
        long currentTime = System.currentTimeMillis();
        try {
            saveIndex(batch, fi);
        } catch (Exception e) {
            LOGGER.error("backup message index error", e);
            retry(batch, fi);
        } finally {
            Metrics.timer("BatchBackup.Store.Timer", TYPE_ARRAY, INDEX_TYPE).update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    private void retry(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> fi) {
        batch.forEach(index -> retry(index, fi));
    }

    private static void monitorBackupIndexQps(String subject) {
        Metrics.meter("backup.message.index.qps", SUBJECT_ARRAY, new String[]{subject}).mark();
    }

    @Override
    protected int getBatchSize() {
        return config.getInt(MESSAGE_BATCH_SIZE_CONFIG_KEY, DEFAULT_BATCH_SIZE);
    }
}
