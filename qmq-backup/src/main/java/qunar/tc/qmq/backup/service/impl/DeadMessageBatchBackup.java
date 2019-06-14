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

import com.google.common.base.Throwables;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.*;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;
import static qunar.tc.qmq.utils.RetrySubjectUtils.getConsumerGroup;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-10 16:53
 */
public class DeadMessageBatchBackup extends AbstractBatchBackup<MessageQueryIndex> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadMessageBatchBackup.class);

    private final KvStore deadMessageStore;
    private final ExecutorService executorService;
    private final BackupKeyGenerator keyGenerator;

    public DeadMessageBatchBackup(KvStore deadMessageStore, BackupKeyGenerator keyGenerator, BackupConfig config) {
        super("deadMessageBackup", config);
        this.deadMessageStore = deadMessageStore;
        this.keyGenerator = keyGenerator;
        this.executorService = Executors.newFixedThreadPool(config.getDynamicConfig().getInt(DEAD_MESSAGE_BACKUP_THREAD_SIZE_CONFIG_KEY, DEFAULT_BACKUP_THREAD_SIZE)
                , new NamedThreadFactory("dead-message-backup"));
    }

    @Override
    protected void doStop() {
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("dead message backup close interrupted.", e);
        }
        try {
            if (deadMessageStore != null) deadMessageStore.close();
        } catch (Exception e) {
            LOGGER.error("close {} failed.", deadMessageStore);
        }
    }

    @Override
    protected void store(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> fi) {
        try {
            doStore(batch, fi);
        } catch (RejectedExecutionException e) {
            LOGGER.error("dead message backup reject exec.", e);
            retry(batch, fi);
        }
    }

    private void retry(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> fi) {
        batch.forEach(message -> retry(message, fi));
    }

    private void retry(MessageQueryIndex message, Consumer<MessageQueryIndex> fi) {
        final int tryStoreNum = message.getBackupRetryTimes();
        if (tryStoreNum < retryNum()) {
            monitorStoreRetry(message.getSubject());
            message.setBackupRetryTimes(tryStoreNum + 1);
            add(message, fi);
        } else {
            monitorStoreDiscard(message.getSubject());
            LOGGER.warn("dead_message backup store discard. subject={}, messageId={}", message.getSubject(), message.getMessageId());
        }
    }

    private void doStore(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> func) {
        executorService.execute(() -> {
            try {
                doBatchSaveBackupDeadMessage(batch, func);
            } catch (Exception e) {
                LOGGER.error("dead message backup store error.", e);
                retry(batch, func);
            }
        });
    }

    private void doBatchSaveBackupDeadMessage(List<MessageQueryIndex> indexes, Consumer<MessageQueryIndex> func) {
        byte[][] recordKeys = new byte[indexes.size()][];
        byte[][][] recordValues = new byte[indexes.size()][][];
        long currentTime = System.currentTimeMillis();
        try {
            MessageQueryIndex tailIndex = null;
            for (int i = 0; i < indexes.size(); ++i) {
                MessageQueryIndex index = indexes.get(i);
                String subject = index.getSubject();
                try {
                    monitorDeadMessageQps(subject);

                    final String realSubject = RetrySubjectUtils.getRealSubject(subject);
                    final String messageId = index.getMessageId();
                    final String consumerGroup = getConsumerGroup(subject);
                    final Date createTime = new Date();
                    final byte[] key = keyGenerator.generateDeadMessageKey(realSubject, messageId, consumerGroup, createTime);
                    final byte[] messageIdBytes = Bytes.UTF8(messageId);
                    final int messageIdLength = messageIdBytes.length;
                    final byte[] value = new byte[16 + brokerGroupLength + messageIdLength];
                    Bytes.setLong(value, index.getSequence(), 0);
                    Bytes.setInt(value, brokerGroupLength, 8);
                    System.arraycopy(brokerGroupBytes, 0, value, 12, brokerGroupLength);
                    Bytes.setInt(value, messageIdLength, 12 + brokerGroupLength);
                    System.arraycopy(messageIdBytes, 0, value, 16 + brokerGroupLength, messageIdLength);

                    final byte[][] recordValue = new byte[][]{value};
                    recordKeys[i] = key;
                    recordValues[i] = recordValue;

                    if (tailIndex == null || tailIndex.compareTo(index) < 0) tailIndex = index;
                } catch (Exception e) {
                    LOGGER.error("batch backup dead message failed.", e);
                    monitorDeadMessageError(subject);
                }
            }

            deadMessageStore.batchSave(recordKeys, recordValues);
            if (func != null) func.accept(tailIndex);
        } catch (Throwable e) {
            LOGGER.error("put backup dead message fail.", e);
            Throwables.propagate(e);
        } finally {
            Metrics.timer("BatchBackup.Store.Timer", TYPE_ARRAY, DEAD_MESSAGE_TYPE).update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    private int retryNum() {
        return config.getInt(MESSAGE_RETRY_NUM_CONFIG_KEY, DEFAULT_RETRY_NUM);
    }

    private static void monitorDeadMessageError(String subject) {
        Metrics.counter("dead_message_store_error", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private static void monitorStoreDiscard(String subject) {
        Metrics.counter("dead_message_backup_store_discard", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private static void monitorDeadMessageQps(String subject) {
        Metrics.meter("backup.dead.message.qps", SUBJECT_ARRAY, new String[]{subject}).mark();
    }

    private static void monitorStoreRetry(String subject) {
        Metrics.counter("dead_message_backup_store_retry", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    @Override
    protected int getBatchSize() {
        return config.getInt(MESSAGE_BATCH_SIZE_CONFIG_KEY, DEFAULT_BATCH_SIZE);
    }
}
