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

import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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
    private final SnapshotStore<IndexCheckpoint> indexCheckpointStore;
    private final SnapshotStore<Long> indexIterateCheckpointStore;
    private final SnapshotStore<Long> syncActionCheckpointStore;

    private final Lock messageCheckpointGuard;
    private final Lock actionCheckpointGuard;
    private final Lock indexCheckpointGuard;
    private final MessageCheckpoint messageCheckpoint;
    private final ActionCheckpoint actionCheckpoint;
    private final IndexCheckpoint indexCheckpoint;
    private final long indexIterateCheckpoint;
    private final AtomicLong syncActionCheckpoint;

    public CheckpointManager(final BrokerRole role, final StorageConfig config, final CheckpointLoader loader) {
        this.messageCheckpointSerde = new MessageCheckpointSerde();
        this.actionCheckpointSerde = new ActionCheckpointSerde();

        this.messageCheckpointStore = new SnapshotStore<>("message-checkpoint", config, messageCheckpointSerde);
        this.actionCheckpointStore = new SnapshotStore<>("action-checkpoint", config, actionCheckpointSerde);
        this.indexCheckpointStore = new SnapshotStore<>("index-checkpoint", config, new IndexCheckpointSerde());
        this.indexIterateCheckpointStore = new SnapshotStore<>("index-iterate-checkpoint", config, new LongSerde());
        this.syncActionCheckpointStore = new SnapshotStore<>("sync-action-checkpoint", config, new LongSerde());

        this.messageCheckpointGuard = new ReentrantLock();
        this.actionCheckpointGuard = new ReentrantLock();
        this.indexCheckpointGuard = new ReentrantLock();

        final MessageCheckpoint messageCheckpoint = loadMessageCheckpoint();
        final ActionCheckpoint actionCheckpoint = loadActionCheckpoint();
        this.indexCheckpoint = loadIndexCheckpoint();
        this.indexIterateCheckpoint = loadIndexIterateCheckpoint();
        this.syncActionCheckpoint = new AtomicLong(loadSyncActionCheckpoint());
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
            return snapshot.getData();
        }
    }

    private ActionCheckpoint loadActionCheckpoint() {
        final Snapshot<ActionCheckpoint> snapshot = actionCheckpointStore.latestSnapshot();
        if (snapshot == null) {
            LOG.info("no action log replay snapshot, return empty state.");
            return new ActionCheckpoint(-1, HashBasedTable.create());
        } else {
            return snapshot.getData();
        }
    }

    private IndexCheckpoint loadIndexCheckpoint() {
        Snapshot<IndexCheckpoint> snapshot = indexCheckpointStore.latestSnapshot();
        if (snapshot == null) {
            LOG.info("no index checkpoint snapshot,return empty state.");
            return new IndexCheckpoint(-1L, -1L);
        }
        return snapshot.getData();
    }

    private long loadIndexIterateCheckpoint() {
        Snapshot<Long> snapshot = indexIterateCheckpointStore.latestSnapshot();
        if (snapshot == null) return 0;
        return snapshot.getData();
    }

    private long loadSyncActionCheckpoint() {
        Snapshot<Long> snapshot = syncActionCheckpointStore.latestSnapshot();
        if (snapshot == null) return 0;
        return snapshot.getData();
    }


    private boolean needSyncCheckpoint(final BrokerRole role, final MessageCheckpoint messageCheckpoint, final ActionCheckpoint actionCheckpoint) {
        if (role != BrokerRole.SLAVE) {
            return false;
        }

        return messageCheckpoint.getOffset() < 0 && actionCheckpoint.getOffset() < 0;
    }

    Table<String, String, ConsumerGroupProgress> allConsumerGroupProgresses() {
        actionCheckpointGuard.lock();
        try {
            return actionCheckpoint.getProgresses();
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    ConsumerGroupProgress getConsumerGroupProgress(String subject, String consumerGroup) {
        actionCheckpointGuard.lock();
        try {
            return actionCheckpoint.getProgresses().get(subject, consumerGroup);
        } finally {
            actionCheckpointGuard.unlock();
        }
    }


    long getMaxPulledMessageSequence(final String partitionName, final String consumerGroup) {
        actionCheckpointGuard.lock();
        try {
            final ConsumerGroupProgress progress = actionCheckpoint.getProgresses().get(partitionName, consumerGroup);
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
        final String consumerGroup = action.group();

        final long maxSequence = getMaxPulledMessageSequence(subject, consumerGroup);
        if (maxSequence + 1 < action.getFirstMessageSequence()) {
            long num = action.getFirstMessageSequence() - maxSequence;
            LOG.warn("Maybe lost message. Last message sequence: {}. Current start sequence {} {}:{}", maxSequence, action.getFirstMessageSequence(), subject, consumerGroup);
            QMon.maybeLostMessagesCountInc(subject, consumerGroup, num);
        }
        final long lastMessageSequence = action.getLastMessageSequence();
        if (maxSequence < lastMessageSequence) {
            updateMaxPulledMessageSequence(subject, consumerGroup, action.isExclusiveConsume(), lastMessageSequence);
        }
    }

    private void updateMaxPulledMessageSequence(final String subject, final String consumerGroup, final boolean isExclusiveConsume, final long maxSequence) {
        final ConsumerGroupProgress progress = getOrCreateConsumerGroupProgress(subject, consumerGroup, isExclusiveConsume);
        progress.setPull(maxSequence);
    }

    private void updateConsumerMaxPullLogSequence(final PullAction action) {
        final String subject = action.subject();
        final String consumerGroup = action.group();
        final String consumerId = action.consumerId();

        final long maxSequence = getConsumerMaxPullLogSequence(subject, consumerGroup, consumerId);
        if (maxSequence + 1 < action.getFirstSequence()) {
            LOG.warn("Pull log not continuous. Last pull log sequence: {}. Current start pull log sequence {} {}:{}:{}", maxSequence, action.getFirstSequence(), subject, consumerGroup, consumerId);
        }

        final long lastSequence = action.getLastSequence();
        if (maxSequence < lastSequence) {
            updateConsumerMaxPullLogSequence(subject, consumerGroup, consumerId, action.isExclusiveConsume(), lastSequence);
        }
    }

    private long getConsumerMaxPullLogSequence(final String subject, final String consumerGroup, final String consumerId) {
        final ConsumerProgress consumer = getConsumerProgress(subject, consumerGroup, consumerId);
        if (consumer == null) {
            return -1;
        } else {
            return consumer.getPull();
        }
    }

    private void updateConsumerMaxPullLogSequence(final String subject, final String consumerGroup, final String consumerId, final boolean isExclusiveConsume, final long maxSequence) {
        final ConsumerProgress consumer = getOrCreateConsumerProgress(subject, consumerGroup, consumerId, isExclusiveConsume);
        consumer.setPull(maxSequence);
    }

    private ConsumerProgress getOrCreateConsumerProgress(final String subject, final String consumerGroup, final String consumerId, final boolean isExclusiveConsume) {
        final ConsumerGroupProgress progress = getOrCreateConsumerGroupProgress(subject, consumerGroup, isExclusiveConsume);

        String exclusiveKey = isExclusiveConsume ? consumerGroup : consumerId;
        final Map<String, ConsumerProgress> consumers = progress.getConsumers();
        if (!consumers.containsKey(exclusiveKey)) {
            consumers.put(exclusiveKey, new ConsumerProgress(subject, consumerGroup, exclusiveKey, -1, -1));
        }
        return consumers.get(exclusiveKey);
    }

    public void updateActionReplayState(final long offset, final RangeAckAction action) {
        actionCheckpointGuard.lock();
        try {
            final String subject = action.subject();
            final String consumerGroup = action.group();
            //if exclusive consume, consumerId == consumerGroup
            final String consumerId = action.consumerId();

            final long maxSequence = getConsumerMaxAckedPullLogSequence(subject, consumerGroup, consumerId);
            if (maxSequence + 1 < action.getFirstSequence()) {
                LOG.warn("Maybe lost ack. Last acked sequence: {}. Current start acked sequence {} {}:{}:{}", maxSequence, action.getFirstSequence(), subject, consumerGroup, consumerId);
            }

            final long lastSequence = action.getLastSequence();
            if (maxSequence < lastSequence) {
                updateConsumerMaxAckedPullLogSequence(subject, consumerGroup, consumerId, lastSequence);
            }

            actionCheckpoint.setOffset(offset);
        } finally {
            actionCheckpointGuard.unlock();
        }
    }

    private long getConsumerMaxAckedPullLogSequence(final String subject, final String consumerGroup, final String consumerId) {
        final ConsumerProgress consumer = getConsumerProgress(subject, consumerGroup, consumerId);
        if (consumer == null) {
            return -1;
        } else {
            return consumer.getAck();
        }
    }

    private void updateConsumerMaxAckedPullLogSequence(final String subject, final String consumerGroup, final String consumerId, final long maxSequence) {
        final ConsumerProgress consumer = getConsumerProgress(subject, consumerGroup, consumerId);
        if (consumer != null) {
            consumer.setAck(maxSequence);
        }
    }

    private ConsumerProgress getConsumerProgress(final String subject, final String consumerGroup, final String consumerId) {
        final ConsumerGroupProgress progress = actionCheckpoint.getProgresses().get(subject, consumerGroup);
        if (progress == null) {
            return null;
        }

        final Map<String, ConsumerProgress> consumers = progress.getConsumers();
        if (consumers == null) {
            return null;
        }

        return consumers.get(consumerId);
    }

    private ConsumerGroupProgress getOrCreateConsumerGroupProgress(final String subject, final String consumerGroup, final boolean isExclusiveConsume) {
        final Table<String, String, ConsumerGroupProgress> progresses = actionCheckpoint.getProgresses();
        if (!progresses.contains(subject, consumerGroup)) {
            final ConsumerGroupProgress progress = new ConsumerGroupProgress(subject, consumerGroup, isExclusiveConsume, -1, new HashMap<>());
            progresses.put(subject, consumerGroup, progress);

        }
        return progresses.get(subject, consumerGroup);
    }

    void removeConsumerProgress(String subject, String consumerGroup, String consumerId) {
        final ConsumerGroupProgress progress = actionCheckpoint.getProgresses().get(subject, consumerGroup);
        if (progress == null) {
            return;
        }

        final Map<String, ConsumerProgress> consumers = progress.getConsumers();
        if (consumers != null) {
            consumers.remove(consumerId);
        }
    }

    void updateMessageReplayState(final MessageLogRecord meta) {
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
                final String partitionName = progress.getPartitionName();
                final String consumerGroup = progress.getConsumerGroup();
                progresses.put(partitionName, consumerGroup, new ConsumerGroupProgress(partitionName, consumerGroup, progress.isExclusiveConsume(), progress.getPull(), consumersCopy));
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

    long getIndexCheckpointIndexOffset() {
        indexCheckpointGuard.lock();
        try {
            return indexCheckpoint.getIndexOffset();
        } finally {
            indexCheckpointGuard.unlock();
        }
    }

    void updateMessageIndexCheckpoint(final long msgOffset) {
        indexCheckpointGuard.lock();
        try {
            if (msgOffset <= indexCheckpoint.getMsgOffset()) return;
            indexCheckpoint.setMsgOffset(msgOffset);
        } finally {
            indexCheckpointGuard.unlock();
        }
    }

    void updateIndexCheckpoint(final long msgOffset, final long indexOffset) {
        indexCheckpointGuard.lock();
        try {
            if (msgOffset <= indexCheckpoint.getMsgOffset()) return;
            indexCheckpoint.setMsgOffset(msgOffset);
            indexCheckpoint.setIndexOffset(indexOffset);
        } finally {
            indexCheckpointGuard.unlock();
        }
    }

    void updateIndexLogCheckpoint(final long indexOffset) {
        indexCheckpointGuard.lock();
        try {
            indexCheckpoint.setIndexOffset(indexOffset);
        } finally {
            indexCheckpointGuard.unlock();
        }
    }

    public long getIndexCheckpointMessageOffset() {
        indexCheckpointGuard.lock();
        try {
            return indexCheckpoint.getMsgOffset();
        } finally {
            indexCheckpointGuard.unlock();
        }
    }

    public long getIndexIterateCheckpoint() {
        return indexIterateCheckpoint;
    }

    public void saveIndexIterateCheckpointSnapshot(final Snapshot<Long> snapshot) {
        if (snapshot.getVersion() <= 0) return;
        indexIterateCheckpointStore.saveSnapshot(snapshot);
    }

    public long getSyncActionLogOffset() {
        return this.syncActionCheckpoint.longValue();
    }

    public void setSyncActionLogOffset(long offset) {
        this.syncActionCheckpoint.set(offset);
    }

    public void addSyncActionLogOffset(int delta) {
        this.syncActionCheckpoint.getAndAdd(delta);
    }

    public void saveSyncActionCheckpointSnapshot() {
        long actionCheckpoint = syncActionCheckpoint.longValue();
        if (actionCheckpoint <= 0) return;
        syncActionCheckpointStore.saveSnapshot(new Snapshot<>(actionCheckpoint, actionCheckpoint));
    }

    Snapshot<IndexCheckpoint> createIndexCheckpoint() {
        IndexCheckpoint indexCheckpoint = duplicateIndexCheckpoint();
        return new Snapshot<>(indexCheckpoint.getMsgOffset(), indexCheckpoint);
    }

    private IndexCheckpoint duplicateIndexCheckpoint() {
        indexCheckpointGuard.lock();
        try {
            return new IndexCheckpoint(indexCheckpoint.getMsgOffset(), indexCheckpoint.getIndexOffset());
        } finally {
            indexCheckpointGuard.unlock();
        }
    }

    void saveIndexCheckpointSnapshot(final Snapshot<IndexCheckpoint> snapshot) {
        if (snapshot.getVersion() < 0) {
            return;
        }
        indexCheckpointStore.saveSnapshot(snapshot);
    }

    Map<String, Long> allMessageMaxSequences() {
        messageCheckpointGuard.lock();
        try {
            return messageCheckpoint.getMaxSequences();
        } finally {
            messageCheckpointGuard.unlock();
        }
    }

    void updateMessageCheckpoint(final long offset, final Map<String, Long> currentMaxSequences) {
        messageCheckpointGuard.lock();
        try {
            currentMaxSequences.forEach((subject, maxSequence) -> {
                messageCheckpoint.getMaxSequences().merge(subject, maxSequence, Math::max);
            });
            messageCheckpoint.setOffset(offset);
        } finally {
            messageCheckpointGuard.unlock();
        }
    }

    private static class LongSerde implements Serde<Long> {

        @Override
        public byte[] toBytes(Long value) {
            return value.toString().getBytes();
        }

        @Override
        public Long fromBytes(byte[] data) {
            return Long.valueOf(new String(data));
        }
    }

    private static class IndexCheckpointSerde implements Serde<IndexCheckpoint> {
        private static final String SLASH = "/";

        @Override
        public byte[] toBytes(IndexCheckpoint value) {
            return (value.getMsgOffset() + SLASH + value.getIndexOffset()).getBytes(Charsets.UTF_8);
        }

        @Override
        public IndexCheckpoint fromBytes(byte[] data) {
            try {
                final String checkpoint = new String(data, Charsets.UTF_8);
                int pos = checkpoint.indexOf(SLASH);
                long msgOffset = Long.parseLong(checkpoint.substring(0, pos));
                long indexOffset = Long.parseLong(checkpoint.substring(pos + 1));
                return new IndexCheckpoint(msgOffset, indexOffset);
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
