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
import com.google.common.collect.Lists;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.ActionEnum;
import qunar.tc.qmq.backup.base.ActionRecord;
import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.backup.store.RocksDBStore;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.*;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-06 17:33
 */
public class RecordBatchBackup extends AbstractBatchBackup<ActionRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordBatchBackup.class);

    private static final String[] SUBJECT_TYPE_ARRAY = new String[]{"subject", "type"};

    private final MetaInfoService metaInfoService;
    private final BackupKeyGenerator keyGenerator;
    private final RocksDBStore rocksDBStore;
    private final KvStore recordStore;

    private final String brokerGroup;

    public RecordBatchBackup(MetaInfoService metaInfoService, BackupConfig backupConfig, BackupKeyGenerator keyGenerator, RocksDBStore rocksDBStore, KvStore recordStore) {
        super("actionBackup", backupConfig);
        this.metaInfoService = metaInfoService;
        this.keyGenerator = keyGenerator;
        this.rocksDBStore = rocksDBStore;
        this.recordStore = recordStore;
        this.brokerGroup = backupConfig.getBrokerGroup();
    }

    @Override
    protected void doStop() {
        try {
            if (rocksDBStore != null) rocksDBStore.close();
        } catch (IOException e) {
            LOGGER.error("close {} failed.", rocksDBStore);
        }
        try {
            if (recordStore != null) recordStore.close();
        } catch (Exception e) {
            LOGGER.error("close {} failed.", recordStore);
        }
    }

    @Override
    protected void store(List<ActionRecord> batch, Consumer<ActionRecord> fi) {
        doStore(batch);
    }

    private void retry(List<ActionRecord> batch) {
        batch.forEach(this::retry);
    }

    private void doStore(final List<ActionRecord> batch) {
        if (!config.getBoolean(ENABLE_RECORD_CONFIG_KEY, true)) return;
        try {
            List<BackupMessage> messages = processRecord(batch);
            doStoreRecord(messages);
        } catch (Exception e) {
            LOGGER.error("record backup store error.", e);
            retry(batch);
        }
    }

    private void doStoreRecord(List<BackupMessage> messages) {
        byte[][] keys = new byte[messages.size()][];
        byte[][][] values = new byte[messages.size()][][];
        long currentTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < messages.size(); ++i) {
                BackupMessage message = messages.get(i);
                String partitionName = message.getSubject();
                String subject = metaInfoService.getSubject(partitionName);
                try {
                    monitorBackupActionQps(subject);
                    // TODO(zhenwei.liu) 这里需要考虑将 partitionName 往哪里放
                    final String consumerGroup = message.getConsumerGroup();
                    final byte[] key = keyGenerator.generateRecordKey(subject, message.getSequence(), brokerGroup, consumerGroup, Bytes.UTF8(Byte.toString(message.getAction())));
                    final String consumerId = message.getConsumerId();
                    final byte[] consumerIdBytes = Bytes.UTF8(consumerId);
                    final int consumerIdBytesLength = consumerIdBytes.length;
                    final byte[] consumerGroupBytes = Bytes.UTF8(consumerGroup);
                    final int consumerGroupLength = consumerGroupBytes.length;
                    final long timestamp = message.getTimestamp();
                    byte[] value = new byte[12 + consumerIdBytesLength + consumerGroupLength];
                    Bytes.setLong(value, timestamp, 0);
                    Bytes.setShort(value, (short) consumerIdBytesLength, 8);
                    System.arraycopy(consumerIdBytes, 0, value, 10, consumerIdBytesLength);
                    Bytes.setShort(value, (short) consumerGroupLength, 10 + consumerIdBytesLength);
                    System.arraycopy(consumerGroupBytes, 0, value, 12 + consumerIdBytesLength, consumerGroupLength);

                    keys[i] = key;
                    values[i] = new byte[][]{value};
                } catch (Exception e) {
                    LOGGER.error("batch backup record failed.", e);
                    monitorBackupActionError(subject);
                }
            }
            recordStore.batchSave(keys, values);
        } catch (Throwable e) {
            LOGGER.error("put backup action fail.", e);
            Throwables.propagate(e);
        } finally {
            Metrics.timer("BatchBackup.Store.Timer", TYPE_ARRAY, RECORD_TYPE).update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    private List<BackupMessage> processRecord(List<ActionRecord> records) {
        final List<BackupMessage> batch = Lists.newArrayList();
        for (ActionRecord record : records) {
            if (record.getAction() instanceof PullAction) {
                onPullAction(record, batch);
            } else if (record.getAction() instanceof RangeAckAction) {
                onAckAction(record, batch);
            }
        }
        return batch;
    }

    private void onAckAction(ActionRecord record, List<BackupMessage> batch) {
        final RangeAckAction ackAction = (RangeAckAction) record.getAction();
        final String prefix = getActionPrefix(ackAction);
        String partitionName = ackAction.subject();
        String subject = metaInfoService.getSubject(partitionName);
        for (long ackLogOffset = ackAction.getFirstSequence(); ackLogOffset <= ackAction.getLastSequence(); ackLogOffset++) {
            monitorAckAction(subject, ackAction.group());
            final Optional<String> consumerLogSequenceOptional = rocksDBStore.get(prefix + "$" + ackLogOffset);
            if (!consumerLogSequenceOptional.isPresent()) {
                monitorMissConsumerLogSeq(subject, ackAction.group());
                continue;
            }
            long consumerLogSequence = Long.parseLong(consumerLogSequenceOptional.get());
            final BackupMessage message = generateBaseMessage(ackAction, consumerLogSequence);
            message.setAction(ActionEnum.ACK.getCode());
            batch.add(message);
        }
    }

    private void onPullAction(ActionRecord record, List<BackupMessage> batch) {
        final PullAction pullAction = (PullAction) record.getAction();
        storePullLogSequence2ConsumerLogSequence(pullAction);
        createBackupMessagesForPullAction(pullAction, batch);
    }

    private void createBackupMessagesForPullAction(final PullAction pullAction, final List<BackupMessage> batch) {
        String partitionName = pullAction.subject();
        final String subject = metaInfoService.getSubject(partitionName);
        for (long pullLogOffset = pullAction.getFirstSequence(); pullLogOffset <= pullAction.getLastSequence(); pullLogOffset++) {
            monitorPullAction(subject, pullAction.group());
            final Long consumerLogSequence = getConsumerLogSequence(pullAction, pullLogOffset);
            final BackupMessage message = generateBaseMessage(pullAction, consumerLogSequence);
            message.setAction(ActionEnum.PULL.getCode());
            batch.add(message);
        }
    }

    private Long getConsumerLogSequence(PullAction action, long pullLogOffset) {
        return action.getFirstMessageSequence() + (pullLogOffset - action.getFirstSequence());
    }

    private void storePullLogSequence2ConsumerLogSequence(PullAction pullAction) {
        final String prefix = getActionPrefix(pullAction);
        for (long i = pullAction.getFirstSequence(); i <= pullAction.getLastSequence(); ++i) {
            final String key = prefix + "$" + i;
            final String messageSequence = Long.toString(pullAction.getFirstMessageSequence() + (i - pullAction.getFirstSequence()));
            rocksDBStore.put(key, messageSequence);
        }
    }

    private BackupMessage generateBaseMessage(Action action, long sequence) {
        final BackupMessage message = new BackupMessage();
        String partitionName = action.subject();
        String subject = metaInfoService.getSubject(partitionName);
        message.setSubject(subject);
        message.setConsumerGroup(action.group());
        message.setConsumerId(action.consumerId());
        message.setTimestamp(action.timestamp());
        message.setSequence(sequence);
        return message;
    }

    private String getActionPrefix(final Action action) {
        return action.subject() + "$" + action.group() + "$" + action.consumerId();
    }

    private void retry(ActionRecord record) {
        final String type = record.getAction().getClass().getSimpleName();
        final int retryNum = record.getRetryNum();
        final String subject = record.getAction().subject();

        if (retryNum < config.getInt(RECORD_BACKUP_RETRY_NUM_CONFIG_KEY, DEFAULT_RETRY_NUM)) {
            monitorStoreRetry(subject, type);
            record.setRetryNum(retryNum + 1);
            add(record, null);
        } else {
            monitorStoreDiscard(subject, type);
        }
    }

    private static void monitorBackupActionQps(String subject) {
        Metrics.meter("backup.action.qps", SUBJECT_ARRAY, new String[]{subject}).mark();
    }

    private static void monitorBackupActionError(String subject) {
        Metrics.counter("record_store_error", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private static void monitorPullAction(String subject, String group) {
        Metrics.counter("on_pull_action", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc();
    }

    private static void monitorMissConsumerLogSeq(String subject, String group) {
        Metrics.meter("AckAction.ConsumerLogSeq.Miss", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).mark();
    }

    private static void monitorAckAction(String subject, String group) {
        Metrics.counter("on_ack_action", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc();
    }

    private static void monitorStoreDiscard(String subject, String type) {
        Metrics.counter("action_backup_store_discard", SUBJECT_TYPE_ARRAY, new String[]{subject, type}).inc();
    }

    private static void monitorStoreRetry(String subject, String type) {
        Metrics.counter("action_backup_store_retry", SUBJECT_TYPE_ARRAY, new String[]{subject, type}).inc();
    }

    @Override
    protected int getBatchSize() {
        return config.getInt(RECORD_BATCH_SIZE_CONFIG_KEY, DEFAULT_BATCH_SIZE);
    }
}
