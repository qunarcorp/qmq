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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.backup.base.*;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.RecordStore;
import qunar.tc.qmq.backup.util.BackupMessageKeyRangeBuilder;
import qunar.tc.qmq.backup.util.BackupMessageKeyRegexpBuilder;
import qunar.tc.qmq.backup.util.KeyValueList;
import qunar.tc.qmq.backup.util.KeyValueListImpl;
import qunar.tc.qmq.utils.Bytes;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static qunar.tc.qmq.backup.service.BackupKeyGenerator.*;
import static qunar.tc.qmq.backup.util.DateTimeUtils.localDateTime2Date;
import static qunar.tc.qmq.backup.util.HBaseValueDecoder.getMessageMeta;
import static qunar.tc.qmq.backup.util.HBaseValueDecoder.getRecord;
import static qunar.tc.qmq.backup.util.KeyTools.generateDecimalFormatKey19;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class HBaseRecordStore extends HBaseStore implements RecordStore {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseRecordStore.class);

    private static final RecordQueryResult EMPTY_RECORD_RESULT = new RecordQueryResult(Collections.emptyList());
    private static final int CONSUMER_GROUP_INDEX_IN_RETRY_MESSAGE = MESSAGE_SUBJECT_LENGTH + MESSAGE_ID_LENGTH + CREATE_TIME_LENGTH + BROKER_GROUP_LENGTH;

    private byte[] indexTable;
    private DicService dicService;
    private BackupKeyGenerator keyGenerator;

    HBaseRecordStore(byte[] table, byte[] indexTable, byte[] family, byte[][] qualifiers, HBaseClient client
            , DicService dicService, BackupKeyGenerator keyGenerator) {
        super(table, family, qualifiers, client);
        this.indexTable = indexTable;
        this.dicService = dicService;
        this.keyGenerator = keyGenerator;
    }

    @Override
    public RecordQueryResult findRecords(RecordQuery query) {
        final String subject = query.getSubject();
        if (Strings.isNullOrEmpty(subject)) return EMPTY_RECORD_RESULT;

        final byte recordCode = query.getRecordCode();
        if (recordCode == RecordEnum.RECORD.getCode()) {
            final String brokerGroup = query.getBrokerGroup();
            if (Strings.isNullOrEmpty(brokerGroup)) return EMPTY_RECORD_RESULT;
            final List<RecordQueryResult.Record> records = findRecords(subject, new BackupMessageMeta(query.getSequence(), query.getBrokerGroup()), recordCode);
            return retResult(records);
        } else if (recordCode == RecordEnum.RETRY_RECORD.getCode()) {
            final String messageId = query.getMessageId();
            if (Strings.isNullOrEmpty(messageId)) return EMPTY_RECORD_RESULT;
            return findRetryRecord(RetryPartitionUtils.buildRetryPartitionName(subject), messageId);
        } else if (recordCode == RecordEnum.DEAD_RECORD.getCode()) {
            final String messageId = query.getMessageId();
            if (Strings.isNullOrEmpty(messageId)) return EMPTY_RECORD_RESULT;
            final List<RecordQueryResult.Record> deads = findDeadRecord(subject, messageId);
            if (CollectionUtils.isEmpty(deads)) return EMPTY_RECORD_RESULT;
            return retResult(deads);
        }

        return null;
    }

    private List<RecordQueryResult.Record> findDeadRecord(final String subject, final String messageId) {
        final String subjectId = dicService.name2Id(subject);
        final String keyRegexp = BackupMessageKeyRegexpBuilder.buildDeadRecordRegexp(subjectId, messageId);
        final String startKey = BackupMessageKeyRangeBuilder.buildDeadRecordStartKey(subjectId, messageId);
        final String endKey = BackupMessageKeyRangeBuilder.buildDeadRecordEndKey(subjectId, messageId);

        try {
            return scan(table, keyRegexp, startKey, endKey, 100, 0, R_FAMILY, B_RECORD_QUALIFIERS, kvs -> {
                final KeyValueList<KeyValue> kvl = new KeyValueListImpl(kvs);
                final byte[] value = kvl.getValue(RECORDS);
                final long sequence = Bytes.getLong(value, 0);
                final long timestamp = Bytes.getLong(value, 8);
                final int consumerGroupLength = value.length - 16;
                final byte[] consumerGroupBytes = new byte[consumerGroupLength];
                System.arraycopy(value, 16, consumerGroupBytes, 0, consumerGroupLength);
                final String consumerGroup = CharsetUtils.toUTF8String(consumerGroupBytes);
                return new RecordQueryResult.Record(consumerGroup, ActionEnum.OMIT.getCode(), RecordEnum.DEAD_RECORD.getCode(), timestamp, "", sequence);
            });
        } catch (Exception e) {
            LOG.error("Failed to find dead records.", e);
            return Collections.emptyList();
        }
    }


    private RecordQueryResult findRetryRecord(final String subject, final String messageId) {
        final List<BackupMessageMeta> metas = scanMessageMeta(subject, messageId);
        final List<RecordQueryResult.Record> records = Lists.newArrayListWithCapacity(metas.size());
        for (BackupMessageMeta meta : metas) {
            final List<RecordQueryResult.Record> retryRecords = findRetryRecords(subject, meta, RecordEnum.RETRY_RECORD.getCode());
            if (!CollectionUtils.isEmpty(retryRecords)) records.addAll(retryRecords);
        }

        return new RecordQueryResult(records);
    }

    private List<RecordQueryResult.Record> findRetryRecords(String subject, BackupMessageMeta meta, byte type) {
        final List<RecordQueryResult.Record> records = Lists.newArrayList();
        try {
            final long sequence = meta.getSequence();
            final String sequenceId = generateDecimalFormatKey19(sequence);
            final String brokerGroup = meta.getBrokerGroup();
            final String consumerGroupId = meta.getConsumerGroupId();
            final String subjectId = dicService.name2Id(subject);
            final String brokerGroupId = dicService.name2Id(brokerGroup);
            final String pullAction = Byte.toString(ActionEnum.PULL.getCode());
            final String ackAction = Byte.toString(ActionEnum.ACK.getCode());

            final byte[] subjectBytes = toUtf8(subjectId);
            final byte[] sequenceBytes = toUtf8(sequenceId);
            final byte[] brokerGroupBytes = toUtf8(brokerGroupId);
            final byte[] consumerGroupBytes = toUtf8(consumerGroupId);

            final byte[] pullKey = keyGenerator.generateRecordKey(subjectBytes, sequenceBytes, brokerGroupBytes, consumerGroupBytes, toUtf8(pullAction));
            final byte[] ackKey = keyGenerator.generateRecordKey(subjectBytes, sequenceBytes, brokerGroupBytes, consumerGroupBytes, toUtf8(ackAction));
            final RecordQueryResult.Record pullRecord = get(table, pullKey, R_FAMILY, B_RECORD_QUALIFIERS, kvs -> getRecord(kvs, type));
            if (pullRecord != null) records.add(pullRecord);
            final RecordQueryResult.Record ackRecord = get(table, ackKey, R_FAMILY, B_RECORD_QUALIFIERS, kvs -> getRecord(kvs, type));
            if (ackRecord != null) records.add(ackRecord);
        } catch (Exception e) {
            LOG.error("find retry records with meta: {} failed.", meta, e);
        }
        return records;
    }

    private List<BackupMessageMeta> scanMessageMeta(String subject, String messageId) {
        final LocalDateTime now = LocalDateTime.now();
        final Date createTimeEnd = localDateTime2Date(now);
        final Date createTimeBegin = localDateTime2Date(now.minusDays(30));

        try {
            final String subjectId = dicService.name2Id(subject);
            final String keyRegexp = BackupMessageKeyRegexpBuilder.buildRetryRegexp(subjectId, messageId);
            final String startKey = BackupMessageKeyRangeBuilder.buildRetryRangeKey(subjectId, messageId, createTimeEnd);
            final String endKey = BackupMessageKeyRangeBuilder.buildRetryRangeKey(subjectId, messageId, createTimeBegin);
            final List<BackupMessageMeta> metas = scan(indexTable, keyRegexp, startKey, endKey, 1000, 0, B_FAMILY, B_MESSAGE_QUALIFIERS, kvs -> {
                KeyValueList<KeyValue> kvl = new KeyValueListImpl(kvs);
                byte[] value = kvl.getValue(CONTENT);
                byte[] rowKey = kvl.getKey();
                BackupMessageMeta meta = getMessageMeta(value);
                if (meta != null && rowKey.length > CONSUMER_GROUP_INDEX_IN_RETRY_MESSAGE) {
                    byte[] consumerGroupId = new byte[CONSUMER_GROUP_LENGTH];
                    System.arraycopy(rowKey, CONSUMER_GROUP_INDEX_IN_RETRY_MESSAGE, consumerGroupId, 0, CONSUMER_GROUP_LENGTH);
                    meta.setConsumerGroupId(new String(consumerGroupId, CharsetUtil.UTF_8));
                }
                return meta;
            });
            return Lists.newArrayList(Sets.newHashSet(metas));
        } catch (Exception e) {
            LOG.error("Failed to scan messages meta.", e);
            return Lists.newArrayList();
        }
    }

    private RecordQueryResult retResult(List<RecordQueryResult.Record> records) {
        if (records != null && records.size() > 0) return new RecordQueryResult(records);
        return new RecordQueryResult(Collections.emptyList());
    }

    // record && retry record && (resend record not included)
    private List<RecordQueryResult.Record> findRecords(String subject, BackupMessageMeta meta, byte type) {
        try {
            final long sequence = meta.getSequence();
            final String brokerGroup = meta.getBrokerGroup();
            final String subjectId = dicService.name2Id(subject);
            final String brokerGroupId = dicService.name2Id(brokerGroup);
            final String recordRegexp = BackupMessageKeyRegexpBuilder.buildRecordRegexp(subjectId, sequence, brokerGroupId);
            final String startKey = BackupMessageKeyRangeBuilder.buildRecordStartKey(subjectId, sequence, brokerGroupId);
            final String endKey = BackupMessageKeyRangeBuilder.buildRecordEndKey(subjectId, sequence, brokerGroupId);
            return scan(table, recordRegexp, startKey, endKey, 1000, 0, R_FAMILY, B_RECORD_QUALIFIERS, kvs -> getRecord(kvs, type));
        } catch (Exception e) {
            LOG.error("Failed to find records.", e);
            return Collections.emptyList();
        }
    }

}
