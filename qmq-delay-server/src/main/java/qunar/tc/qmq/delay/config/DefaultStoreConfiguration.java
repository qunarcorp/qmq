/*
 * Copyright 2018 Qunar
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

package qunar.tc.qmq.delay.config;

import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.constants.BrokerConstants;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-11 9:54
 */
public class DefaultStoreConfiguration implements StoreConfiguration {
    private static final String MESSAGE_LOG = "message_log";
    private static final String SCHEDULE_LOG = "schedule_log";
    private static final String DISPATCH_LOG = "dispatch_log";
    private static final String CHECKPOINT = "checkpoint";
    private static final String PER_MESSAGE_SEGMENT_FILE_SIZE = "per.segment.file.size";
    private static final String PER_MESSAGE_SILE = "per.message.size";
    private static final String LOAD_SEGMENT_DELAY_MIN = "load.segment.delay.min";
    private static final String DISPATCH_LOG_KEEP_HOUR = "dispatch.log.keep.hour";
    private static final String SCHEDULE_CLEAN_BEFORE_DISPATCH_HOUR = "schedule.clean.before.dispatch.hour";
    private static final String LOAD_IN_ADVANCE_MIN = "load.in.advance.min";
    private static final String LOAD_BLOCKING_EXIT_MIN = "load.blocking.exit.min";

    private static final int MESSAGE_SEGMENT_LOG_FILE_SIZE = 1024 * 1024 * 1024;
    private static final int SINGLE_MESSAGE_LIMIT_SIZE = 50 * 1024 * 1024;
    private static final int SEGMENT_LOAD_DELAY_TIMES_IN_MIN = 1;
    private static final int DISPATCH_LOG_KEEP_TIMES_IN_HOUR = 3 * 24;
    private static final int SCHEDULE_CLEAN_BEFORE_DISPATCH_TIMES_IN_HOUR = 24;
    private static final int LOAD_IN_ADVANCE_TIMES_IN_MIN = 30;
    private static final int LOAD_BLOCKING_EXIT_TIMES_IN_MIN = 10;

    private static final long MS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MS_PER_MINUTE = TimeUnit.MINUTES.toMillis(1);

    private final DynamicConfig config;

    public DefaultStoreConfiguration(DynamicConfig config) {
        this.config = config;
    }

    @Override
    public DynamicConfig getConfig() {
        return config;
    }

    @Override
    public String getMessageLogStorePath() {
        return buildStorePath(MESSAGE_LOG);
    }

    @Override
    public String getScheduleLogStorePath() {
        return buildStorePath(SCHEDULE_LOG);
    }

    @Override
    public String getDispatchLogStorePath() {
        return buildStorePath(DISPATCH_LOG);
    }

    @Override
    public int getMessageLogSegmentFileSize() {
        return config.getInt(PER_MESSAGE_SEGMENT_FILE_SIZE, MESSAGE_SEGMENT_LOG_FILE_SIZE);
    }

    @Override
    public int getSingleMessageLimitSize() {
        return config.getInt(PER_MESSAGE_SILE, SINGLE_MESSAGE_LIMIT_SIZE);
    }

    @Override
    public String getCheckpointStorePath() {
        return buildStorePath(CHECKPOINT);
    }

    @Override
    public long getMessageLogRetentionMs() {
        final int retentionHours = config.getInt(BrokerConstants.MESSAGE_LOG_RETENTION_HOURS, BrokerConstants.DEFAULT_MESSAGE_LOG_RETENTION_HOURS);
        return retentionHours * MS_PER_HOUR;
    }

    @Override
    public long getDispatchLogKeepTime() {
        return config.getInt(DISPATCH_LOG_KEEP_HOUR, DISPATCH_LOG_KEEP_TIMES_IN_HOUR) * MS_PER_HOUR;
    }

    @Override
    public long getCheckCleanTimeBeforeDispatch() {
        return config.getInt(SCHEDULE_CLEAN_BEFORE_DISPATCH_HOUR, SCHEDULE_CLEAN_BEFORE_DISPATCH_TIMES_IN_HOUR) * MS_PER_HOUR;
    }

    @Override
    public long getLogCleanerIntervalSeconds() {
        return config.getInt(BrokerConstants.LOG_RETENTION_CHECK_INTERVAL_SECONDS, BrokerConstants.DEFAULT_LOG_RETENTION_CHECK_INTERVAL_SECONDS);
    }

    @Override
    public String getScheduleOffsetCheckpointPath() {
        return buildStorePath(CHECKPOINT);
    }

    @Override
    public long getLoadInAdvanceTimesInMillis() {
        return config.getInt(LOAD_IN_ADVANCE_MIN, LOAD_IN_ADVANCE_TIMES_IN_MIN) * MS_PER_MINUTE;
    }

    @Override
    public long getLoadBlockingExitTimesInMillis() {
        return config.getInt(LOAD_BLOCKING_EXIT_MIN, LOAD_BLOCKING_EXIT_TIMES_IN_MIN) * MS_PER_MINUTE;
    }

    @Override
    public boolean isDeleteExpiredLogsEnable() {
        return config.getBoolean(BrokerConstants.ENABLE_DELETE_EXPIRED_LOGS, false);
    }

    @Override
    public int getLoadSegmentDelayMinutes() {
        return config.getInt(LOAD_SEGMENT_DELAY_MIN, SEGMENT_LOAD_DELAY_TIMES_IN_MIN);
    }

    private String buildStorePath(final String name) {
        final String root = config.getString(BrokerConstants.STORE_ROOT, BrokerConstants.LOG_STORE_ROOT);
        return new File(root, name).getAbsolutePath();
    }
}
