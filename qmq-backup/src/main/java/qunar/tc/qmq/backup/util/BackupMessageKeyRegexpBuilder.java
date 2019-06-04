package qunar.tc.qmq.backup.util;

import com.google.common.base.Strings;

import static qunar.tc.qmq.backup.util.KeyTools.generateDecimalFormatKey19;
import static qunar.tc.qmq.backup.util.KeyTools.generateMD5Key;

public class BackupMessageKeyRegexpBuilder {
    public static String build(String subjectId, String messageId) {
        if (Strings.isNullOrEmpty(subjectId)) throw new RuntimeException("Subject Needed.");
        StringBuilder builder = new StringBuilder();
        if (!Strings.isNullOrEmpty(messageId)) {
            builder.append("^").append(subjectId).append("\\d{12}").append(generateMD5Key(messageId));
        } else {
            builder.append("^").append(subjectId).append("\\d{12}").append("\\w{32}");
        }
        return builder.toString();
    }

    public static String buildRetryRegexp(String subjectId, String messageId) {
        if (Strings.isNullOrEmpty(subjectId) || Strings.isNullOrEmpty(messageId))
            throw new RuntimeException("Subject And MessageId Needed.");

        return "^" + subjectId + generateMD5Key(messageId) + "\\d{12}" + "\\d{6}" + "\\d{6}" + "\\d{19}";
    }

    public static String buildDeadRegexp(String subjectId, String consumerGroupId) {
        if (Strings.isNullOrEmpty(subjectId) || Strings.isNullOrEmpty(consumerGroupId))
            throw new RuntimeException("Subject And ConsumerGroup Needed.");
        return "^" + subjectId + consumerGroupId + "\\d{12}" + "\\w{32}";
    }

    public static String buildDeadRecordRegexp(String subjectId, String messageId) {
        return "^" + subjectId + generateMD5Key(messageId) + "\\d{6}$";
    }

    public static String buildRecordRegexp(String subjectId, long sequence, String brokerGroupId) {
        return "^" + subjectId + generateDecimalFormatKey19(sequence) + brokerGroupId + "\\d{6}\\d{1}$";
    }
}
