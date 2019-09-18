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

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_BATCH_SIZE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_RETRY_NUM;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.RECORD_BACKUP_RETRY_NUM_CONFIG_KEY;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.RECORD_BATCH_SIZE_CONFIG_KEY;
import static qunar.tc.qmq.metrics.MetricsConstants.EMPTY;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Throwables;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetryPartitionUtils;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-10 16:53
 */
public class DeadRecordBatchBackup extends AbstractBatchBackup<MessageQueryIndex> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadRecordBatchBackup.class);

    private final KvStore recordStore;
    private final BackupKeyGenerator keyGenerator;

    public DeadRecordBatchBackup(KvStore recordStore, BackupKeyGenerator keyGenerator, BackupConfig config) {
        super("deadRecordBackup", config);
        this.recordStore = recordStore;
        this.keyGenerator = keyGenerator;
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void store(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> fi) {
        doStore(batch);
    }

	private void doStore(final List<MessageQueryIndex> batch) {
		for (int i = 0; i < retryNum(); ++i) {
			try {
				doBatchSaveBackupDeadRecord(batch);
				return;
			}
			catch (Exception e) {
				LOGGER.error("dead record backup store error.", e);
				monitorStoreRetry();
			}
		}

		monitorStoreDiscard();

	}

    private void doBatchSaveBackupDeadRecord(List<MessageQueryIndex> messages) {
        final byte[][] recordKeys = new byte[messages.size()][];
        final byte[][][] recordValues = new byte[messages.size()][][];
        long currentTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < messages.size(); ++i) {
                final MessageQueryIndex message = messages.get(i);
                try {
                    final String subject = message.getSubject();
                    final String realSubject = RetryPartitionUtils.getRealPartitionName(subject);
                    final String consumerGroup = RetryPartitionUtils.getConsumerGroup(subject);
                    final byte[] key = keyGenerator.generateDeadRecordKey(RetryPartitionUtils.buildDeadRetryPartitionName(realSubject), message.getMessageId(), consumerGroup);
                    final long createTime = message.getCreateTime() + 500; //为了让死消息的action排在最后面
                    final long sequence = message.getSequence();
                    final byte[] consumerGroupBytes = Bytes.UTF8(consumerGroup);
                    byte[] value = new byte[16 + consumerGroupBytes.length];
                    Bytes.setLong(value, sequence, 0);
                    Bytes.setLong(value, createTime, 8);
                    System.arraycopy(consumerGroupBytes, 0, value, 16, consumerGroupBytes.length);
                    byte[][] recordValue = new byte[][]{value};
                    recordKeys[i] = key;
                    recordValues[i] = recordValue;
                } catch (Exception e) {
                    LOGGER.error("batch backup dead record failed.");
                    monitorStoreDeadDeadError(message.getSubject());
                }
            }
            recordStore.batchSave(recordKeys, recordValues);
        } catch (Throwable e) {
            LOGGER.error("put backup dead record fail.", e);
            Throwables.propagate(e);
        } finally {
            Metrics.timer("BatchBackup.Store.Timer", TYPE_ARRAY, DEAD_RECORD_TYPE).update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    private int retryNum() {
        return config.getInt(RECORD_BACKUP_RETRY_NUM_CONFIG_KEY, DEFAULT_RETRY_NUM);
    }

    private void monitorStoreDiscard() {
        Metrics.counter("dead_record_backup_store_discard", EMPTY, EMPTY).inc();
    }

    private void monitorStoreDeadDeadError(String subject) {
        Metrics.counter("dead_record_store_error", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private static void monitorStoreRetry() {
        Metrics.counter("dead_record_backup_store_retry", EMPTY, EMPTY).inc();
    }

    @Override
    protected int getBatchSize() {
        return config.getInt(RECORD_BATCH_SIZE_CONFIG_KEY, DEFAULT_BATCH_SIZE);
    }
}
