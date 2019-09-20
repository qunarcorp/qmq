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
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_BATCH_SIZE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.MESSAGE_BATCH_SIZE_CONFIG_KEY;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;
import static qunar.tc.qmq.utils.RetryPartitionUtils.getConsumerGroup;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-10 16:53
 */
public class DeadMessageContentBatchBackup extends AbstractBatchBackup<MessageQueryIndex> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadMessageContentBatchBackup.class);

    private final KvStore deadMessageStore;
    private final BackupKeyGenerator keyGenerator;
    private final MessageService messageService;


    public DeadMessageContentBatchBackup(KvStore deadMessageStore, BackupKeyGenerator keyGenerator, BackupConfig config, MessageService messageService) {
        super("deadMessageContentBackup", config);
        this.deadMessageStore = deadMessageStore;
        this.keyGenerator = keyGenerator;
        this.messageService = messageService;
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void store(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> fi) {
        doStore(batch, fi);
    }

    private void doStore(List<MessageQueryIndex> batch, Consumer<MessageQueryIndex> func) {
        byte[][] recordKeys = new byte[batch.size()][];
        byte[][][] recordValues = new byte[batch.size()][][];
        long currentTime = System.currentTimeMillis();
        try {
            MessageQueryIndex tailIndex = null;
            for (int i = 0; i < batch.size(); ++i) {
                MessageQueryIndex index = batch.get(i);
                String subject = index.getSubject();

                BackupQuery query = new BackupQuery();
                query.setSubject(subject);
                query.setBrokerGroup(new String(brokerGroupBytes, CharsetUtil.UTF_8));
                query.setSequence(index.getSequence());

                CompletableFuture<byte[]> future = messageService.findMessageBytes(query);


                try {
                    monitorDeadMessageQps(subject);
                    LOGGER.info("backup dead msg content, subject: {}", subject);
                    final String realSubject = RetryPartitionUtils.getRealPartitionName(subject);
                    final String messageId = index.getMessageId();
                    final String consumerGroup = getConsumerGroup(subject);
                    final Date createTime = new Date(index.getCreateTime());
                    final byte[] key = keyGenerator.generateDeadMessageKey(realSubject, messageId, consumerGroup, createTime);

                    byte[] value = future.get(1, TimeUnit.SECONDS);


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
            Metrics.timer("BatchBackup.Store.Timer", TYPE_ARRAY, DEAD_MESSAGE_CONTENT_TYPE).update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    private static void monitorDeadMessageError(String subject) {
        Metrics.counter("dead_message_content_store_error", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    private static void monitorDeadMessageQps(String subject) {
        Metrics.meter("backup.dead.message.content.qps", SUBJECT_ARRAY, new String[]{subject}).mark();
    }

    @Override
    protected int getBatchSize() {
        return config.getInt(MESSAGE_BATCH_SIZE_CONFIG_KEY, DEFAULT_BATCH_SIZE);
    }
}
