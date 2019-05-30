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

    private static final RecordResult EMPTY_RECORD_RESULT = new RecordResult(Collections.emptyList());
    private static final int CONSUMER_GROUP_INDEX_IN_RETRY_MESSAGE = MESSAGE_SUBJECT_LENGTH + MESSAGE_ID_LENGTH + CREATE_TIME_LENGTH + BROKER_GROUP_LENGTH;

    private byte[] indexTable;
    private DicService dicService;
    private BackupKeyGenerator keyGenerator;

    public HBaseRecordStore(byte[] table, byte[] indexTable, byte[] family, byte[][] qualifiers, HBaseClient client
            , DicService dicService, BackupKeyGenerator keyGenerator) {
        super(table, family, qualifiers, client);
        this.indexTable = indexTable;
        this.dicService = dicService;
        this.keyGenerator = keyGenerator;
    }

    @Override
    public RecordResult findRecords(RecordQuery query) {
        final String subject = query.getSubject();
        if (Strings.isNullOrEmpty(subject)) return EMPTY_RECORD_RESULT;

        byte recordCode = query.getRecordCode();
        if (recordCode == RecordEnum.RECORD.getCode()) {
            final String brokerGroup = query.getBrokerGroup();
            if (Strings.isNullOrEmpty(brokerGroup)) return EMPTY_RECORD_RESULT;
            List<RecordResult.Record> records = findRecords(subject, new BackupMessageMeta(query.getSequence(), query.getBrokerGroup()), RecordEnum.RECORD.getCode());
            return retResult(records);
        } else if (recordCode == RecordEnum.RETRY_RECORD.getCode()) {
            String messageId = query.getMessageId();
            if (Strings.isNullOrEmpty(messageId)) return EMPTY_RECORD_RESULT;
            return findRetryRecord(subject, messageId);
        }

        return null;
    }

    private RecordResult findRetryRecord(final String subject, final String messageId) {
        List<BackupMessageMeta> metas = scanMessageMeta(subject, messageId);
        List<RecordResult.Record> records = Lists.newArrayListWithCapacity(metas.size());
        for (BackupMessageMeta meta : metas) {
            List<RecordResult.Record> retryRecords = findRetryRecords(subject, meta, RecordEnum.RETRY_RECORD.getCode());
            if (!CollectionUtils.isEmpty(retryRecords)) records.addAll(retryRecords);
        }

        return new RecordResult(records);
    }

    private List<RecordResult.Record> findRetryRecords(String subject, BackupMessageMeta meta, byte type) {
        final long sequence = meta.getSequence();
        final String sequenceId = generateDecimalFormatKey19(sequence);
        final String brokerGroup = meta.getBrokerGroup();
        final String consumerGroupId = meta.getConsumerGroupId();
        final String subjectId = dicService.name2Id(subject);
        final String brokerGroupId = dicService.name2Id(brokerGroup);
        final String pullAction = Byte.toString(ActionEnum.PULL.getCode());
        final String ackAction = Byte.toString(ActionEnum.ACK.getCode());

        List<RecordResult.Record> records = Lists.newArrayList();
        byte[] subjectBytes = toUtf8(subjectId);
        byte[] sequenceBytes = toUtf8(sequenceId);
        byte[] brokerGroupBytes = toUtf8(brokerGroupId);
        byte[] consumerGroupBytes = toUtf8(consumerGroupId);

        final byte[] pullKey = keyGenerator.generateRecordKey(subjectBytes, sequenceBytes, brokerGroupBytes, consumerGroupBytes, toUtf8(pullAction));
        final byte[] ackKey = keyGenerator.generateRecordKey(subjectBytes, sequenceBytes, brokerGroupBytes, consumerGroupBytes, toUtf8(ackAction));
        try {
            RecordResult.Record pullRecord = get(table, pullKey, R_FAMILY, B_RECORD_QUALIFIERS, kvs -> getRecord(kvs, type));
            if (pullRecord != null) records.add(pullRecord);
            RecordResult.Record ackRecord = get(table, ackKey, R_FAMILY, B_RECORD_QUALIFIERS, kvs -> getRecord(kvs, type));
            if (ackRecord != null) records.add(ackRecord);
        } catch (Exception e) {
            LOG.error("find retry records with meta: {} failed.", meta, e);
        }
        return records;
    }

    private List<BackupMessageMeta> scanMessageMeta(String subject, String messageId) {
        LocalDateTime now = LocalDateTime.now();
        Date createTimeEnd = localDateTime2Date(now);
        Date createTimeBegin = localDateTime2Date(now.minusDays(30));

        final String subjectId = dicService.name2Id(subject);
        final String keyRegexp = BackupMessageKeyRegexpBuilder.buildRetryRegexp(subjectId, messageId);
        final String startKey = BackupMessageKeyRangeBuilder.buildRetryRangeKey(subjectId, messageId, createTimeEnd);
        final String endKey = BackupMessageKeyRangeBuilder.buildRetryRangeKey(subjectId, messageId, createTimeBegin);

        try {
            List<BackupMessageMeta> metas = scan(indexTable, keyRegexp, startKey, endKey, 1000, 0, B_FAMILY, B_MESSAGE_QUALIFIERS, kvs -> {
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

    private RecordResult retResult(List<RecordResult.Record> records) {
        if (records != null && records.size() > 0) return new RecordResult(records);
        return new RecordResult(Collections.emptyList());
    }

    // record && retry record && (resend record not included)
    private List<RecordResult.Record> findRecords(String subject, BackupMessageMeta meta, byte type) {
        final long sequence = meta.getSequence();
        final String brokerGroup = meta.getBrokerGroup();
        final String subjectId = dicService.name2Id(subject);
        final String brokerGroupId = dicService.name2Id(brokerGroup);
        final String recordRegexp = BackupMessageKeyRegexpBuilder.buildRecordRegexp(subjectId, sequence, brokerGroupId);
        final String startKey = BackupMessageKeyRangeBuilder.buildRecordStartKey(subjectId, sequence, brokerGroupId);
        final String endKey = BackupMessageKeyRangeBuilder.buildRecordEndKey(subjectId, sequence, brokerGroupId);

        try {
            return scan(table, recordRegexp, startKey, endKey, 1000, 0, R_FAMILY, B_RECORD_QUALIFIERS, kvs -> getRecord(kvs, type));
        } catch (Exception e) {
            LOG.error("Failed to find records.", e);
            return Collections.emptyList();
        }
    }

}