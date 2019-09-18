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

package qunar.tc.qmq.backup.service;

import com.google.common.base.Strings;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.Date;

import static java.lang.System.arraycopy;
import static qunar.tc.qmq.backup.util.KeyTools.*;

/**
 * @author yiqun.fan create on 17-10-30.
 */
public class BackupKeyGenerator {
    public static final short MESSAGE_SUBJECT_LENGTH = 6;
    public static final short MESSAGE_ID_LENGTH = 32;
    public static final short RECORD_SEQUENCE_LENGTH = 19;
    public static final short CREATE_TIME_LENGTH = 12;
    public static final short CONSUMER_GROUP_LENGTH = 6;
    public static final short BROKER_GROUP_LENGTH = 6;

    private final DicService dicService;

    public BackupKeyGenerator(DicService dicService) {
        this.dicService = dicService;
    }

    @SuppressWarnings({"SameParameterValue", "Duplicates"})
    private byte[] generateRowKey(int length0, int index0, int length1, int index1, int length2, int index2, int length3, int index3, int length4, int index4, int length5, int index5
            , byte[] value0, byte[] value1, byte[] value2, byte[] value3, byte[] value4, byte[] value5) {
        byte[] key = new byte[length0 + length1 + length2 + length3 + length4 + length5];
        arraycopy(value0, 0, key, index0, value0.length);
        arraycopy(value1, 0, key, index1, value1.length);
        arraycopy(value2, 0, key, index2, value2.length);
        arraycopy(value3, 0, key, index3, value3.length);
        arraycopy(value4, 0, key, index4, value4.length);
        arraycopy(value5, 0, key, index5, value5.length);
        return key;
    }

    @SuppressWarnings({"SameParameterValue", "Duplicates"})
    private byte[] generateRowKey(int length0, int index0, int length1, int index1, int length2, int index2, int length3, int index3, int length4, int index4
            , byte[] value0, byte[] value1, byte[] value2, byte[] value3, byte[] value4) {
        byte[] key = new byte[length0 + length1 + length2 + length3 + length4];
        arraycopy(value0, 0, key, index0, value0.length);
        arraycopy(value1, 0, key, index1, value1.length);
        arraycopy(value2, 0, key, index2, value2.length);
        arraycopy(value3, 0, key, index3, value3.length);
        arraycopy(value4, 0, key, index4, value4.length);
        return key;
    }

    @SuppressWarnings({"SameParameterValue", "Duplicates"})
    private byte[] generateRowKey(int length0, int index0, int length1, int index1, int length2, int index2, int length3, int index3
            , byte[] value0, byte[] value1, byte[] value2, byte[] value3) {
        byte[] key = new byte[length0 + length1 + length2 + length3];
        arraycopy(value0, 0, key, index0, value0.length);
        arraycopy(value1, 0, key, index1, value1.length);
        arraycopy(value2, 0, key, index2, value2.length);
        arraycopy(value3, 0, key, index3, value3.length);
        return key;
    }

    @SuppressWarnings("SameParameterValue")
    private byte[] generateRowKey(int length0, int index0, int length1, int index1, int length2, int index2,
                                  byte[] value0, byte[] value1, byte[] value2) {
        byte[] key = new byte[length0 + length1 + length2];
        arraycopy(value0, 0, key, index0, value0.length);
        arraycopy(value1, 0, key, index1, value1.length);
        arraycopy(value2, 0, key, index2, value2.length);
        return key;
    }

    public static byte[] toUtf8(final String s) {
        return CharsetUtils.toUTF8Bytes(s);
    }

    public byte[] generateDeadMessageKey(String subject, String messageId, String consumerGroup, Date createTime) {
        String subjectId = dicService.name2Id(subject);
        String consumerGroupId = dicService.name2Id(consumerGroup);
        String createTimeKey = generateDateKey(createTime);
        String messageIdKey = generateMD5Key(messageId);

        return generateDeadMessageKey(toUtf8(subjectId), toUtf8(consumerGroupId), toUtf8(createTimeKey), toUtf8(messageIdKey));
    }

    private byte[] generateDeadMessageKey(byte[] subject, byte[] consumerGroup, byte[] createTime, byte[] messageId) {
        final short subjectIndex = 0;
        final short consumerGroupIndex = subjectIndex + MESSAGE_SUBJECT_LENGTH;
        final short createTimeIndex = consumerGroupIndex + CONSUMER_GROUP_LENGTH;
        final short messageIdIndex = createTimeIndex + CREATE_TIME_LENGTH;
        return generateRowKey(MESSAGE_SUBJECT_LENGTH, subjectIndex
                , CONSUMER_GROUP_LENGTH, consumerGroupIndex
                , CREATE_TIME_LENGTH, createTimeIndex
                , MESSAGE_ID_LENGTH, messageIdIndex
                , subject, consumerGroup, createTime, messageId);
    }

    public byte[] generateDeadRecordKey(String subject, String messageId, String consumerGroup) {
        final String subjectId = dicService.name2Id(subject);
        final String messageIdKey = generateMD5Key(messageId);
        final String consumerGroupId = dicService.name2Id(consumerGroup);
        return generateDeadRecordKey(toUtf8(subjectId), toUtf8(messageIdKey), toUtf8(consumerGroupId));
    }

    private byte[] generateDeadRecordKey(byte[] subject, byte[] messageId, byte[] consumerGroup) {
        final short subjectIndex = 0;
        final short messageIdIndex = subjectIndex + MESSAGE_SUBJECT_LENGTH;
        final short consumerGroupIndex = messageIdIndex + MESSAGE_ID_LENGTH;
        return generateRowKey(MESSAGE_SUBJECT_LENGTH, subjectIndex, MESSAGE_ID_LENGTH, messageIdIndex, CONSUMER_GROUP_LENGTH, consumerGroupIndex
                , subject, messageId, consumerGroup);
    }

    public byte[] generateMessageKey(String subject, Date createTime, String messageId, String brokerGroup, String consumerGroup, long sequence) {
        final String subjectId = dicService.name2Id(subject);
        final String createTimeKey = generateDateKey(createTime);
        final String messageIdKey = generateMD5Key(messageId);
        if (RetryPartitionUtils.isRealPartitionName(subject)) {
            return generateMessageKey(toUtf8(subjectId), toUtf8(createTimeKey), toUtf8(messageIdKey));
        }
        if (RetryPartitionUtils.isRetryPartitionName(subject)) {
            String sequenceId = generateDecimalFormatKey19(sequence);
            String brokerGroupId = dicService.name2Id(brokerGroup);
            String consumerGroupId = dicService.name2Id(consumerGroup);
            if (!Strings.isNullOrEmpty(consumerGroupId))
                return generateRetryMessageKey(toUtf8(subjectId), toUtf8(messageIdKey), toUtf8(createTimeKey), toUtf8(brokerGroupId), toUtf8(consumerGroupId), toUtf8(sequenceId));
        }
        throw new RuntimeException("Unsupported subject type, subject: " + subject);
    }

    private byte[] generateRetryMessageKey(byte[] subject, byte[] messageId, byte[] createTime, byte[] brokerGroup, byte[] consumerGroup, byte[] sequence) {
        final short subjectIndex = 0;
        final short messageIdIndex = subjectIndex + MESSAGE_SUBJECT_LENGTH;
        final short createTimeIndex = messageIdIndex + MESSAGE_ID_LENGTH;
        final short brokerGroupIndex = createTimeIndex + CREATE_TIME_LENGTH;
        final short consumerGroupIndex = brokerGroupIndex + BROKER_GROUP_LENGTH;
        final short sequenceIndex = consumerGroupIndex + CONSUMER_GROUP_LENGTH;
        return generateRowKey(MESSAGE_SUBJECT_LENGTH, subjectIndex
                , MESSAGE_ID_LENGTH, messageIdIndex
                , CREATE_TIME_LENGTH, createTimeIndex
                , BROKER_GROUP_LENGTH, brokerGroupIndex
                , CONSUMER_GROUP_LENGTH, consumerGroupIndex
                , RECORD_SEQUENCE_LENGTH, sequenceIndex
                , subject, messageId, createTime, brokerGroup, consumerGroup, sequence);
    }

    private byte[] generateMessageKey(byte[] subject, byte[] createTime, byte[] messageId) {
        final short subjectIndex = 0;
        final short createTimeIndex = subjectIndex + MESSAGE_SUBJECT_LENGTH;
        final short messageIdIndex = createTimeIndex + CREATE_TIME_LENGTH;
        return generateRowKey(MESSAGE_SUBJECT_LENGTH, subjectIndex
                , CREATE_TIME_LENGTH, createTimeIndex
                , MESSAGE_ID_LENGTH, messageIdIndex
                , subject, createTime, messageId);

    }

    public byte[] generateRecordKey(String subject, long sequence, String brokerGroup, String consumerGroup, byte[] action) {
        final String subjectId = dicService.name2Id(subject);
        final String sequenceId = generateDecimalFormatKey19(sequence);
        final String brokerGroupId = dicService.name2Id(brokerGroup);
        final String consumerGroupId = dicService.name2Id(consumerGroup);
        return generateRecordKey(toUtf8(subjectId), toUtf8(sequenceId), toUtf8(brokerGroupId), toUtf8(consumerGroupId), action);
    }

    public byte[] generateRecordKey(byte[] subject, byte[] sequence, byte[] brokerGroup, byte[] consumerGroup, byte[] action) {
        final short subjectIndex = 0;
        final short sequenceIndex = subjectIndex + MESSAGE_SUBJECT_LENGTH;
        final short brokerGroupIndex = sequenceIndex + RECORD_SEQUENCE_LENGTH;
        final short consumerGroupIndex = brokerGroupIndex + BROKER_GROUP_LENGTH;
        final short actionIndex = consumerGroupIndex + CONSUMER_GROUP_LENGTH;

        return generateRowKey(MESSAGE_SUBJECT_LENGTH, subjectIndex, RECORD_SEQUENCE_LENGTH, sequenceIndex,
                brokerGroup.length, brokerGroupIndex, CONSUMER_GROUP_LENGTH, consumerGroupIndex, action.length, actionIndex,
                subject, sequence, brokerGroup, consumerGroup, action);
    }
}
