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

import com.google.common.collect.Table;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.store.action.ActionEvent;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2017/7/4
 */
public interface Storage extends Disposable {
    void start();

    StorageConfig getStorageConfig();

    PutMessageResult appendMessage(final RawMessage message);

    SegmentBuffer getMessageData(final long wroteOffset);

    GetMessageResult getMessage(String subject, long sequence);

    GetMessageResult pollMessages(String subject, long startSequence, int maxMessages);

    GetMessageResult pollMessages(final String subject, final long startSequence, final int maxMessages, MessageFilter filter);

    long getMaxMessageOffset();

    long getMinMessageOffset();

    long getMaxActionLogOffset();

    long getMinActionLogOffset();

    long getMaxMessageSequence(final String subject);

    PutMessageResult putAction(final Action action);

    List<PutMessageResult> putPullLogs(final String subject, final String group, final String consumerId, final List<PullLogMessage> messages);

    CheckpointManager getCheckpointManager();

    ConsumerGroupProgress getConsumerGroupProgress(final String subject, final String group);

    Table<String, String, ConsumerGroupProgress> allConsumerGroupProgresses();

    long getMaxPulledMessageSequence(String partitionName, String group);

    long getMessageSequenceByPullLog(final String subject, final String group, final String consumerId, final long pullLogSequence);

    void updateConsumeQueue(String subject, String group, int consumeFromWhereCode);

    ConsumeQueue locateConsumeQueue(final String subject, final String group);

    Map<String, ConsumeQueue> locateSubjectConsumeQueues(final String subject);

    <T> void registerEventListener(final Class<T> clazz, final FixedExecOrderEventBus.Listener<T> listener);

    void registerActionEventListener(final FixedExecOrderEventBus.Listener<ActionEvent> listener);

    SegmentBuffer getActionLogData(final long offset);

    boolean appendMessageLogData(final long startOffset, final ByteBuffer data);

    boolean appendActionLogData(final long startOffset, final ByteBuffer data);

    MessageLogRecordVisitor newMessageLogVisitor(final long startOffset);

    void disableLagMonitor(String subject, String group);

    Table<String, String, PullLog> allPullLogs();

    void destroyPullLog(final String partitionName, final String group, final String consumerId);
}
