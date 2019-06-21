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

package qunar.tc.qmq.constants;

/**
 * User: zhaohuiyu Date: 5/13/13 Time: 4:05 PM
 */
public class BrokerConstants {
    public static final String PORT_CONFIG = "broker.port";
    public static final Integer DEFAULT_PORT = 20881;

    public static final String META_SERVER_ENDPOINT = "meta.server.endpoint";

    public static final String STORE_ROOT = "store.root";
    public static final String LOG_STORE_ROOT = "/data";

    public static final String MESSAGE_LOG_RETENTION_HOURS = "messagelog.retention.hours";
    public static final int DEFAULT_MESSAGE_LOG_RETENTION_HOURS = 72; // 3 days

    public static final String CONSUMER_LOG_RETENTION_HOURS = "consumerlog.retention.hours";
    public static final int DEFAULT_CONSUMER_LOG_RETENTION_HOURS = 72; // 3 days

    public static final String CONSUMER_LOG_V2_ENABLE = "consumerlog.v2.enable";

    public static final String SMT_RETENTION_HOURS = "smt.retention.hours";
    public static final int DEFAULT_SMT_RETENTION_HOURS = 72; // 3 days

    public static final String SMT_ENABLE = "smt.enable";

    public static final String RETRY_DELAY_SECONDS = "message.retry.delay.seconds";
    public static final int DEFAULT_RETRY_DELAY_SECONDS = 5;
    public static final String LOG_RETENTION_CHECK_INTERVAL_SECONDS = "log.retention.check.interval.seconds";
    public static final int DEFAULT_LOG_RETENTION_CHECK_INTERVAL_SECONDS = 60;
    public static final String ENABLE_DELETE_EXPIRED_LOGS = "log.expired.delete.enable";

    // for messages mem table
    public static final String MAX_RESERVED_MEMTABLE = "memtable.max_reserved";
    public static final int DEFAULT_MAX_RESERVED_MEMTABLE = 2;

    public static final String MAX_ACTIVE_MEMTABLE = "memtable.max_active";
    public static final int DEFAULT_MAX_ACTIVE_MEMTABLE = 4;

    public static final String LOG_DISPATCHER_PAUSE_MILLIS = "log_dispatcher.pause.millis";

    // slave
    public static final long DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS = 1000L;


    public static String PULL_LOG_RETENTION_HOURS = "pulllog.retention.hours";
    public static int DEFAULT_PULL_LOG_RETENTION_HOURS = 72; // 3 days

    public static String CHECKPOINT_RETAIN_COUNT = "checkpoint.retain.count";
    public static int DEFAULT_CHECKPOINT_RETAIN_COUNT = 5;

    public static final String ACTION_CHECKPOINT_INTERVAL = "action.checkpoint.interval";
    public static final long DEFAULT_ACTION_CHECKPOINT_INTERVAL = 10_000;

    public static final String MESSAGE_CHECKPOINT_INTERVAL = "message.checkpoint.interval";
    public static final long DEFAULT_MESSAGE_CHECKPOINT_INTERVAL = 10_000;
}
