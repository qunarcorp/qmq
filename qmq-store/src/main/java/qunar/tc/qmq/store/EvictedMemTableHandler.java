/*
 * Copyright 2019 Qunar, Inc.
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

import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.store.MessageMemTableManager.MemTableEvictedCallback;
import qunar.tc.qmq.store.SortedMessagesTable.TabletBuilder;
import qunar.tc.qmq.store.buffer.MemTableBuffer;
import qunar.tc.qmq.store.result.Result;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author keli.wang
 * @since 2019-06-17
 */
class EvictedMemTableHandler implements MemTableEvictedCallback {
    private final SortedMessagesTable smt;
    private final ConsumerLogManager consumerLogManager;
    private final CheckpointManager checkpointManager;
    private final ExecutorService flushExecutor;

    EvictedMemTableHandler(final SortedMessagesTable smt, final ConsumerLogManager consumerLogManager, final CheckpointManager checkpointManager) {
        this.smt = smt;
        this.consumerLogManager = consumerLogManager;
        this.checkpointManager = checkpointManager;
        this.flushExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("smt-flusher"));
    }

    @Override
    public boolean onEvicted(final MessageMemTable table) {
        return smt.newTabletBuilder(table.getTabletId())
                .map(builder -> buildTablet(builder, table))
                .orElse(false);
    }

    private boolean buildTablet(final TabletBuilder builder, final MessageMemTable table) {
        if (!builder.begin(table.getTotalDataSize(), table.getBeginOffset(), table.getEndOffset())) {
            return false;
        }

        final Map<String, Long> maxSequences = new HashMap<>();
        for (final MessageMemTable.Entry entry : table) {
            if (!handleEntry(builder, entry, table.getOverheadBytes(), maxSequences)) {
                return false;
            }
        }

        final boolean ok = builder.finish();
        if (!ok) {
            return false;
        } else {
            flushExecutor.submit(() -> {
                builder.flush();
                consumerLogManager.flush();
                writeOffsetAndCheckpoint(table, maxSequences);
            });
            return true;
        }
    }

    private boolean handleEntry(final TabletBuilder builder, final MessageMemTable.Entry entry, final int overheadBytes, final Map<String, Long> maxSequences) {
        final MemTableBuffer data = entry.getData();
        try {
            final Result<TabletBuilder.AppendStatus, Integer> result = builder.append(data.getBuffer());
            switch (result.getStatus()) {
                case SUCCESS:
                    // TODO(keli.wang): write consumer log here or write after all messages done?
                    maxSequences.merge(entry.getSubject(), entry.getSequence(), Math::max);
                    return writeConsumerLog(builder.getTabletId(), entry, overheadBytes, result.getData());
                case ERROR:
                    return false;
                default:
                    throw new RuntimeException("unknown result status " + result.getStatus());
            }
        } finally {
            data.release();
        }
    }

    private boolean writeConsumerLog(final long tabletId, final MessageMemTable.Entry entry, final int overheadBytes, final int position) {
        final ConsumerLog log = consumerLogManager.getOrCreateConsumerLog(entry.getSubject());
        return log.writeSMTIndex(entry.getSequence(), entry.getTimestamp(), tabletId,
                position + overheadBytes, entry.getData().getSize() - overheadBytes);
    }

    private void writeOffsetAndCheckpoint(final MessageMemTable table, final Map<String, Long> maxSequences) {
        createOffsetFile(table);
        updateCheckpoint(table, maxSequences);
    }

    private void createOffsetFile(final MessageMemTable table) {
        final HashMap<String, Long> offsets = new HashMap<>(table.getFirstSequences());
        checkpointManager.allMessageMaxSequences().forEach((subject, offset) -> offsets.merge(subject, offset, Math::max));
        consumerLogManager.createOffsetFileFor(table.getTabletId(), offsets);
    }

    private void updateCheckpoint(final MessageMemTable table, final Map<String, Long> maxSequences) {
        // TODO(keli.wang): merge update and save into one operation
        checkpointManager.updateMessageCheckpoint(table.getEndOffset(), maxSequences);
        final Snapshot<MessageCheckpoint> snapshot = checkpointManager.createMessageCheckpointSnapshot();
        checkpointManager.saveMessageCheckpointSnapshot(snapshot);
    }
}