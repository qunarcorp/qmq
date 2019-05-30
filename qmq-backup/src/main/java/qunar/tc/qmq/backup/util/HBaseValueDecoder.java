package qunar.tc.qmq.backup.util;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.jboss.netty.util.CharsetUtil;
import qunar.tc.qmq.backup.base.BackupMessageMeta;
import qunar.tc.qmq.backup.base.RecordResult;
import qunar.tc.qmq.utils.CharsetUtils;

import java.util.List;

import static qunar.tc.qmq.backup.service.BackupKeyGenerator.MESSAGE_SUBJECT_LENGTH;
import static qunar.tc.qmq.backup.service.BackupKeyGenerator.RECORD_SEQUENCE_LENGTH;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.RECORDS;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/30
 */
public class HBaseValueDecoder {
    public static BackupMessageMeta getMessageMeta(byte[] value) {
        try {
            if (value != null && value.length > 0) {
                long sequence = Bytes.getLong(value, 0);
                long createTime = Bytes.getLong(value, 8);
                int brokerGroupLength = Bytes.getInt(value, 16);
                byte[] brokerGroupBytes = new byte[brokerGroupLength];
                System.arraycopy(value, 20, brokerGroupBytes, 0, brokerGroupLength);
                int messageIdLength = value.length - 20 - brokerGroupLength;
                byte[] messageIdBytes = new byte[messageIdLength];
                System.arraycopy(value, 20 + brokerGroupLength, messageIdBytes, 0, messageIdLength);
                BackupMessageMeta meta = new BackupMessageMeta(sequence, new String(brokerGroupBytes, CharsetUtil.UTF_8), new String(messageIdBytes, CharsetUtil.UTF_8));
                meta.setCreateTime(createTime);
                return meta;
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    public static RecordResult.Record getRecord(List<KeyValue> kvs, byte type) {
        KeyValueList<KeyValue> kvl = new KeyValueListImpl(kvs);
        byte[] rowKey = kvl.getKey();
        String row = CharsetUtils.toUTF8String(rowKey);
        long sequence = Long.parseLong(row.substring(MESSAGE_SUBJECT_LENGTH, MESSAGE_SUBJECT_LENGTH + RECORD_SEQUENCE_LENGTH));
        byte action = Byte.parseByte(row.substring(row.length() - 1));

        byte[] value = kvl.getValue(RECORDS);
        long timestamp = Bytes.getLong(value, 0);
        short consumerIdLength = Bytes.getShort(value, 8);
        byte[] consumerIdBytes = new byte[consumerIdLength];
        System.arraycopy(value, 10, consumerIdBytes, 0, consumerIdLength);
        String consumerId = CharsetUtils.toUTF8String(consumerIdBytes);
        short consumerGroupLength = Bytes.getShort(value, 10 + consumerIdLength);
        byte[] consumerGroupBytes = new byte[consumerGroupLength];
        System.arraycopy(value, 12 + consumerIdLength, consumerGroupBytes, 0, consumerGroupLength);
        String consumerGroup = CharsetUtils.toUTF8String(consumerGroupBytes);
        return new RecordResult.Record(consumerGroup, action, type, timestamp, consumerId, sequence);
    }

}