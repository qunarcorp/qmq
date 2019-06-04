package qunar.tc.qmq.backup.util;

import com.google.common.base.Strings;

import java.util.Date;

import static qunar.tc.qmq.backup.service.DicService.MAX_CONSUMER_GROUP_ID;
import static qunar.tc.qmq.backup.service.DicService.MIN_CONSUMER_GROUP_ID;
import static qunar.tc.qmq.backup.util.KeyTools.*;

public class BackupMessageKeyRangeBuilder {

    public static String buildStartKey(String start, String subjectId, Date msgCreateTimeEnd) {
        if (Strings.isNullOrEmpty(start)) return subjectId + generateDateKey(msgCreateTimeEnd);
        return start;
    }

    public static String buildEndKey(String subjectId, Date msgCreateTimeBegin) {
        return subjectId + generateDateKey(msgCreateTimeBegin);
    }

    public static String buildRetryRangeKey(String subjectId, String messageId, Date createTime) {
        return subjectId + generateMD5Key(messageId) + generateDateKey(createTime);
    }

    public static String buildDeadStartKey(String start, String subjectId, String consumerGroupId, Date msgCreateTimeEnd) {
        if (Strings.isNullOrEmpty(start)) return buildDeadKey(subjectId, consumerGroupId, msgCreateTimeEnd);
        return start;
    }

    public static String buildDeadEndKey(String subjectId, String consumerGroupId, Date msgCreateTimeBegin) {
        return buildDeadKey(subjectId, consumerGroupId, msgCreateTimeBegin);
    }

    private static String buildDeadKey(String subjectId, String consumerGroupId, Date msgCreateTime) {
        return subjectId + consumerGroupId + generateDateKey(msgCreateTime);
    }

    public static String buildDeadRecordStartKey(String subjectId, String messageId) {
        return subjectId + generateMD5Key(messageId) + MIN_CONSUMER_GROUP_ID;
    }

    public static String buildDeadRecordEndKey(String subjectId, String messageId) {
        return subjectId + generateMD5Key(messageId) + MAX_CONSUMER_GROUP_ID;
    }

    public static String buildRecordStartKey(String subjectId, long sequence, String brokerGroupId) {
        return buildRecordRangeKey(subjectId, sequence, brokerGroupId) + MIN_CONSUMER_GROUP_ID;
    }

    private static String buildRecordRangeKey(String subjectId, long sequence, String brokerGroupId) {
        return subjectId + generateDecimalFormatKey19(sequence) + brokerGroupId;
    }

    public static String buildRecordEndKey(String subjectId, long sequence, String brokerGroupId) {
        return buildRecordRangeKey(subjectId, sequence, brokerGroupId) + MAX_CONSUMER_GROUP_ID;
    }
}
