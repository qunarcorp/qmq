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

package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public interface StorageConfig {
    String getCheckpointStorePath();

    String getMessageLogStorePath();

    long getMessageLogRetentionMs();

    String getConsumerLogStorePath();

    long getConsumerLogRetentionMs();

    int getLogRetentionCheckIntervalSeconds();

    String getPullLogStorePath();

    long getPullLogRetentionMs();

    String getActionLogStorePath();

    String getIndexLogStorePath();

    long getLogRetentionMs();

    String getSMTStorePath();

    long getSMTRetentionMs();

    int getRetryDelaySeconds();

    int getCheckpointRetainCount();

    long getActionCheckpointInterval();

    long getMessageCheckpointInterval();

    int getMaxReservedMemTable();

    int getMaxActiveMemTable();

    boolean isConsumerLogV2Enable();

    boolean isSMTEnable();

    long getLogDispatcherPauseMillis();
}
