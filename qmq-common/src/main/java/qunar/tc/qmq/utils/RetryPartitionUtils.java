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

package qunar.tc.qmq.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.List;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public final class RetryPartitionUtils {
    private static final Joiner RETRY_SUBJECT_JOINER = Joiner.on('%');
    private static final Splitter RETRY_SUBJECT_SPLITTER = Splitter.on('%').trimResults().omitEmptyStrings();
    private static final String RETRY_SUBJECT_PREFIX = "%RETRY";
    private static final String DEAD_RETRY_SUBJECT_PREFIX = "%DEAD_RETRY";

    private RetryPartitionUtils() {
    }

    public static boolean isRealPartitionName(final String partitionName) {
        return !Strings.isNullOrEmpty(partitionName) && !isRetryPartitionName(partitionName) && !isDeadRetryPartitionName(partitionName);
    }

    public static String buildRetryPartitionName(final String partitionName, final String consumerGroup) {
        return RETRY_SUBJECT_JOINER.join(RETRY_SUBJECT_PREFIX, partitionName, consumerGroup);
    }

    public static boolean isRetryPartitionName(final String partitionName) {
        return Strings.nullToEmpty(partitionName).startsWith(RETRY_SUBJECT_PREFIX);
    }

    public static String buildDeadRetryPartitionName(final String partitionName, final String consumerGroup) {
        return RETRY_SUBJECT_JOINER.join(DEAD_RETRY_SUBJECT_PREFIX, partitionName, consumerGroup);
    }

    public static boolean isDeadRetryPartitionName(final String partitionName) {
        return Strings.nullToEmpty(partitionName).startsWith(DEAD_RETRY_SUBJECT_PREFIX);
    }

    public static String getRealPartitionName(final String partitionName) {
        final Optional<String> optional = getPartitionNameFromRetry(partitionName);
        if (optional.isPresent()) {
            return optional.get();
        }
        return partitionName;
    }

    private static Optional<String> getPartitionNameFromRetry(final String retryPartitionName) {
        if (!isRetryPartitionName(retryPartitionName) && !isDeadRetryPartitionName(retryPartitionName)) {
            return Optional.absent();
        }
        final List<String> parts = RETRY_SUBJECT_SPLITTER.splitToList(retryPartitionName);
        if (parts.size() != 3) {
            return Optional.absent();
        } else {
            return Optional.of(parts.get(1));
        }
    }

    public static String[] parseSubjectAndGroup(String partitionName) {
        if (!isRetryPartitionName(partitionName) && !isDeadRetryPartitionName(partitionName)) {
            return null;
        }

        final List<String> parts = RETRY_SUBJECT_SPLITTER.splitToList(partitionName);
        if (parts.size() != 3) {
            return null;
        } else {
            return new String[]{parts.get(1), parts.get(2)};
        }
    }

    public static String getConsumerGroup(final String partitionName) {
        if (!isRetryPartitionName(partitionName) && !isDeadRetryPartitionName(partitionName)) return "";
        final List<String> parts = RETRY_SUBJECT_SPLITTER.splitToList(partitionName);
        if (parts.size() != 3) return "";
        else return parts.get(2);
    }

    public static String buildRetryPartitionName(final String partitionName) {
        return RETRY_SUBJECT_JOINER.join(RETRY_SUBJECT_PREFIX, partitionName);
    }

    public static String buildDeadRetryPartitionName(final String partitionName) {
        return RETRY_SUBJECT_JOINER.join(DEAD_RETRY_SUBJECT_PREFIX, partitionName);
    }
}
