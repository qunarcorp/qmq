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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class CheckpointManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);

    private final MessageCheckpointSerde messageCheckpointSerde;
    private final ActionCheckpointSerde actionCheckpointSerde;
    private final SnapshotStore<MessageCheckpoint> messageCheckpointStore;
    private final SnapshotStore<ActionCheckpoint> actionCheckpointStore;

    private final Lock messageCheckpointGuard;
    private final Lock actionCheckpointGuard;
    private final MessageCheckpoint messageCheckpoint;
    private final ActionCheckpoint actionCheckpoint;

    CheckpointManager(final BrokerRole role, final StorageConfig config, final CheckpointLoader loader) {
        this.messageCheckpointSerde = new MessageCheckpointSerde();
        this.actionCheckpointSerde = new ActionCheckpointSerde();

        this.messageCheckpointStore = new SnapshotStore<>("message-checkpoint", config, messageCheckpointSerde);
        this.actionCheckpointStore = new SnapshotStore<>("action-checkpoint", config, actionCheckpointSerde);

        this.messageCheckpointGuard = new ReentrantLock();
        this.actionCheckpointGuard = new ReentrantLock();

        final MessageCheckpoint messageCheckpoint = loadMessageCheckpoint();
        final ActionCheckpoint actionCheckpoint = loadActionCheckpoint();
        if (needSyncCheckpoint(role, messageCheckpoint, actionCheckpoint)) {
            // TODO(keli.wang): must try to cleanup this messy...
            final ByteBuf buf = loader.loadCheckpoint();
            buf.readByte();
            final int messageLength = buf.readInt();
            final byte[] message = new byte[messageLength];
            buf.readBytes(message);
            this.messageCheckpoint = messageCheckpointSerde.fromBytes(message);
            final int actionLength = buf.readInt();
            final byte[] action = new byte[actionLength];
            buf.readBytes(action);
            this.actionCheckpoint = actionCheckpointSerde.fromBytes(action);
        } else {
            this.messageCheckpoint = messageCheckpoint;
            this.actionCheckpoint = actionCheckpoint;
        }
    }

    private MessageCheckpoint loadMessageCheckpoint() {
        final Snapshot<MessageCheckpoint> snapshot = messageCheckpointStore.latestSnapshot();
        if (snapshot == null) {
            LOG.info("no message log replay snapshot, return empty state.");
            return new MessageCheckpoint(-1, new HashMap<>());
        } else {
            final MessageCheckpoint checkpoint = snapshot.getData();
            if (checkpoint.isFromOldVersion()) {
                checkpoint.setOffset(snapshot.getVersion());
            }
            return checkpoint;
        }
    }

    private ActionCheckpoint loadActionCheckpoint() {
        final Snapshot<ActionCheckpoint> snapshot = actionCheckpointStore.latestSnapshot();
        if (snapshot == null) {
            LOG.info("no action log replay snapshot, return empty state.");
            return new ActionCheckpoint(-1, HashBasedTable.create());
        } else {
            final ActionCheckpoint checkpoint = snapshot.getData();
            if (checkpoint.isFromOldVersion()) {
                checkpoint.setOffset(snapshot.getVersion());
            }
            return checkpoint;
        }
    }

    private boolean needSyncCheckpoint(final BrokerRole role, final MessageCheckpoint messageCheckpoint, final ActionCheckpoint actionCheckpoint) {
        if (role != BrokerRole.SLAVE) {
            return false;
        }

        return messageCheckpoint.getOffset() < 0 && actionCheckpoint.getOffset() < 0;
    }

    void fixOldVersionCheckpointIfShould(ConsumerLogManager consumerLogManager, PullLogManager pullLogManager) {
        if (messageCheckpoint.isFromOldVersion()) {
            LOG.info("fix message replay state using consumer log");
            fixMessageCheckpoint(consumerLogManager);
            messageCheckpoint.setFromOldVersion(false);
        }

        if (actionCheckpoint.isFromOldVersion()) {
            LOG.info("fix action replay state using pull log");
            fixActionCheckpoint(pullLogManager);
            messageCheckpoint.setFromOldVersion(false);
        }
    }

    private void fixMessageCheckpoint(ConsumerLogManager manager) {
        final Map<String, Long> maxSequences = messageCheckpoint.getMaxSequences();
        final Map<String, Long> offsets = manager.currentConsumerLogOffset();
        offsets.forEach(maxSequences::put);
    }

    private void fixActionCheckpoint(PullLogManager manager) {
        final Table<String, String, PullLog> allLogs = manager.getLogs();
        final Table<String, String, ConsumerGroupProgress> progresses = actionCheckpoint.getProgresses();
        progresses.values().forEach(progress -> {
            final String subject = progress.getSubject();
            final String group = progress.getGroup();
            final String groupAndSubject = GroupAndSubject.groupAndSubject(subject, group);

            final Map<String, ConsumerProgress> consumers = progress.getConsumers();

            final Map<String, PullLog> logs = allLogs.column(groupAndSubject);
            logs.forEach((consumerId, log) -> {
                final long pull = log.getMaxOffset() - 1;
                final ConsumerProgress consumer = consumers.get(consumerId);
                if (consumer != null) {
                    consumer.setPull(pull);
                } else {
                    consumers.put(consumerId, new ConsumerProgress(subject, group, consumerId, pull, -1));
                }
            });

            if (consumers.size() == 1) {
                consumers.values().forEach(consumer -> {
                    if (consumer.getPull() < 0) {
                        progress.setBroadcast(true);
                        consumer.setPull(progress.getPull());
                    }
                });
            }
        });
    }

    Collection<ConsumerGroupProgress> allConsumerGroupProgresses() {
        actionCheckpointGuard.lock();
        try {
            return actionCheckpoint.getProgresses().values();
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    ConsumerGroupProgress getConsumerGroupProgress(String subject, String group) {
        actionCheckpointGuard.lock();
        try {
            return actionCheckpoint.getProgresses().get(subject, group);
        } finally {
            actionCheckpointGuard.unlock();
        }
    }


    long getMaxPulledMessageSequence(final String subject, final String group) {
        actionCheckpointGuard.lock();
        try {
            final ConsumerGroupProgress progress = actionCheckpoint.getProgresses().get(subject, group);
            if (progress == null) {
                return -1;
            }

            return progress.getPull();
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    public void updateActionReplayState(final long offset, final PullAction action) {
        actionCheckpointGuard.lock();
        try {
            updateMaxPulledMessageSequence(action);
            updateConsumerMaxPullLogSequence(action);
            actionCheckpoint.setOffset(offset);
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    private void updateMaxPulledMessageSequence(final PullAction action) {
        final String subject = action.subject();
        final String group = action.group();

        final long maxSequence = getMaxPulledMessageSequence(subject, group);
        if (maxSequence + 1 < action.getFirstMessageSequence()) {
            long num = action.getFirstMessageSequence() - maxSequence;
            LOG.warn("Maybe lost message. Last message sequence: {}. Current start sequence {} {}:{}", maxSequence, action.getFirstMessageSequence(), subject, group);
            QMon.maybeLostMessagesCountInc(subject, group, num);
        }
        final long lastMessageSequence = action.getLastMessageSequence();
        if (maxSequence < lastMessageSequence) {
            updateMaxPulledMessageSequence(subject, group, action.isBroadcast(), lastMessageSequence);
        }
    }

    private void updateMaxPulledMessageSequence(final String subject, final String group, final boolean broadcast, final long maxSequence) {
        final ConsumerGroupProgress progress = getOrCreateConsumerGroupProgress(subject, group, broadcast);
        progress.setPull(maxSequence);
    }

    private void updateConsumerMaxPullLogSequence(final PullAction action) {
        final String subject = action.subject();
        final String group = action.group();
        final String consumerId = action.consumerId();

        final long maxSequence = getConsumerMaxPullLogSequence(subject, group, consumerId);
        if (maxSequence + 1 < action.getFirstSequence()) {
            LOG.warn("Pull log not continuous. Last pull log sequence: {}. Current start pull log sequence {} {}:{}:{}", maxSequence, action.getFirstSequence(), subject, group, consumerId);
        }

        final long lastSequence = action.getLastSequence();
        if (maxSequence < lastSequence) {
            updateConsumerMaxPullLogSequence(subject, group, consumerId, action.isBroadcast(), lastSequence);
        }
    }

    private long getConsumerMaxPullLogSequence(final String subject, final String group, final String consumerId) {
        final ConsumerProgress consumer = getConsumerProgress(subject, group, consumerId);
        if (consumer == null) {
            return -1;
        } else {
            return consumer.getPull();
        }
    }

    private void updateConsumerMaxPullLogSequence(final String subject, final String group, final String consumerId, final boolean broadcast, final long maxSequence) {
        final ConsumerProgress consumer = getOrCreateConsumerProgress(subject, group, consumerId, broadcast);
        consumer.setPull(maxSequence);
    }

    private ConsumerProgress getOrCreateConsumerProgress(final String subject, final String group, final String consumerId, final boolean broadcast) {
        final ConsumerGroupProgress progress = getOrCreateConsumerGroupProgress(subject, group, broadcast);

        final Map<String, ConsumerProgress> consumers = progress.getConsumers();
        if (!consumers.containsKey(consumerId)) {
            consumers.put(consumerId, new ConsumerProgress(subject, group, consumerId, -1, -1));
        }
        return consumers.get(consumerId);
    }

    public void updateActionReplayState(final long offset, final RangeAckAction action) {
        actionCheckpointGuard.lock();
        try {
            final String subject = action.subject();
            final String group = action.group();
            final String consumerId = action.consumerId();

            final long maxSequence = getConsumerMaxAckedPullLogSequence(subject, group, consumerId);
            if (maxSequence + 1 < action.getFirstSequence()) {
                LOG.warn("Maybe lost ack. Last acked sequence: {}. Current start acked sequence {} {}:{}:{}", maxSequence, action.getFirstSequence(), subject, group, consumerId);
            }

            final long lastSequence = action.getLastSequence();
            if (maxSequence < lastSequence) {
                updateConsumerMaxAckedPullLogSequence(subject, group, consumerId, lastSequence);
            }

            actionCheckpoint.setOffset(offset);
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    private long getConsumerMaxAckedPullLogSequence(final String subject, final String group, final String consumerId) {
        final ConsumerProgress consumer = getConsumerProgress(subject, group, consumerId);
        if (consumer == null) {
            return -1;
        } else {
            return consumer.getAck();
        }
    }

    private void updateConsumerMaxAckedPullLogSequence(final String subject, final String group, final String consumerId, final long maxSequence) {
        final ConsumerProgress consumer = getConsumerProgress(subject, group, consumerId);
        if (consumer != null) {
            consumer.setAck(maxSequence);
        }
    }

    private ConsumerProgress getConsumerProgress(final String subject, final String group, final String consumerId) {
        final ConsumerGroupProgress progress = actionCheckpoint.getProgresses().get(subject, group);
        if (progress == null) {
            return null;
        }

        final Map<String, ConsumerProgress> consumers = progress.getConsumers();
        if (consumers == null) {
            return null;
        }

        return consumers.get(consumerId);
    }

    private ConsumerGroupProgress getOrCreateConsumerGroupProgress(final String subject, final String group, final boolean broadcast) {
        final Table<String, String, ConsumerGroupProgress> progresses = actionCheckpoint.getProgresses();
        if (!progresses.contains(subject, group)) {
            final ConsumerGroupProgress progress = new ConsumerGroupProgress(subject, group, broadcast, -1, new HashMap<>());
            progresses.put(subject, group, progress);

        }
        return progresses.get(subject, group);
    }

    void removeConsumerProgress(String subject, String group, String consumerId) {
        final ConsumerGroupProgress progress = actionCheckpoint.getProgresses().get(subject, group);
        if (progress == null) {
            return;
        }

        final Map<String, ConsumerProgress> consumers = progress.getConsumers();
        if (consumers != null) {
            consumers.remove(consumerId);
        }
    }

    void updateMessageReplayState(final MessageLogMeta meta) {
        messageCheckpointGuard.lock();
        try {
            final String subject = meta.getSubject();
            final long sequence = meta.getSequence();

            final Map<String, Long> sequences = messageCheckpoint.getMaxSequences();
            if (sequences.containsKey(subject)) {
                sequences.merge(subject, sequence, Math::max);
            } else {
                sequences.put(subject, sequence);
            }

            final long offset = meta.getWroteOffset() + meta.getWroteBytes();
            messageCheckpoint.setOffset(offset);
        } finally {
            messageCheckpointGuard.unlock();
        }
    }

    public byte[] dumpMessageCheckpoint() {
        messageCheckpointGuard.lock();
        try {
            return messageCheckpointSerde.toBytes(messageCheckpoint);
        } finally {
            messageCheckpointGuard.unlock();
        }
    }

    public byte[] dumpActionCheckpoint() {
        actionCheckpointGuard.lock();
        try {
            return actionCheckpointSerde.toBytes(actionCheckpoint);
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    Snapshot<MessageCheckpoint> createMessageCheckpointSnapshot() {
        final MessageCheckpoint checkpoint = duplicateMessageCheckpoint();
        return new Snapshot<>(checkpoint.getOffset(), checkpoint);
    }

    Snapshot<ActionCheckpoint> createActionCheckpointSnapshot() {
        final ActionCheckpoint checkpoint = duplicateActionCheckpoint();
        return new Snapshot<>(checkpoint.getOffset(), checkpoint);
    }

    private MessageCheckpoint duplicateMessageCheckpoint() {
        messageCheckpointGuard.lock();
        try {
            return new MessageCheckpoint(messageCheckpoint.getOffset(), new HashMap<>(messageCheckpoint.getMaxSequences()));
        } finally {
            messageCheckpointGuard.unlock();
        }
    }

    private ActionCheckpoint duplicateActionCheckpoint() {
        actionCheckpointGuard.lock();
        try {
            final Table<String, String, ConsumerGroupProgress> progresses = HashBasedTable.create();
            for (final ConsumerGroupProgress progress : actionCheckpoint.getProgresses().values()) {
                final Map<String, ConsumerProgress> consumers = progress.getConsumers();
                if (consumers == null) {
                    continue;
                }

                final Map<String, ConsumerProgress> consumersCopy = new HashMap<>();
                for (final ConsumerProgress consumer : consumers.values()) {
                    consumersCopy.put(consumer.getConsumerId(), new ConsumerProgress(consumer));
                }
                final String subject = progress.getSubject();
                final String group = progress.getGroup();
                progresses.put(subject, group, new ConsumerGroupProgress(subject, group, progress.isBroadcast(), progress.getPull(), consumersCopy));
            }
            final long offset = actionCheckpoint.getOffset();
            return new ActionCheckpoint(offset, progresses);
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    // TODO(keli.wang): update offset and state at the same time within the lock
    public long getActionCheckpointOffset() {
        return actionCheckpoint.getOffset();
    }

    public long getMessageCheckpointOffset() {
        return messageCheckpoint.getOffset();
    }

    void saveMessageCheckpointSnapshot(final Snapshot<MessageCheckpoint> snapshot) {
        if (snapshot.getVersion() < 0) {
            return;
        }
        messageCheckpointStore.saveSnapshot(snapshot);
    }

    void saveActionCheckpointSnapshot(final Snapshot<ActionCheckpoint> snapshot) {
        if (snapshot.getVersion() < 0) {
            return;
        }
        actionCheckpointStore.saveSnapshot(snapshot);
    }

    @Override
    public void close() {
        messageCheckpointStore.close();
        actionCheckpointStore.close();
    }
}
