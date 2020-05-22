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

    public static String buildDeadContentRegexp(String subjectId, String consumerGroupId, String messageId) {
        if (Strings.isNullOrEmpty(subjectId)|| Strings.isNullOrEmpty(consumerGroupId)) {
            throw new RuntimeException("Subject , ConsumerGroup Needed.");
        }

        if (Strings.isNullOrEmpty(messageId)) {
            return "^" + subjectId + consumerGroupId + "\\d{12}" + "\\w{32}";
        }

        return "^" + subjectId +  consumerGroupId + "\\d{12}" + generateMD5Key(messageId);
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
