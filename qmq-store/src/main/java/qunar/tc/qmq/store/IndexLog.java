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

import static qunar.tc.qmq.store.AppendMessageStatus.END_OF_FILE;
import static qunar.tc.qmq.store.AppendMessageStatus.SUCCESS;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import qunar.tc.qmq.metrics.Metrics;

public class IndexLog implements AutoCloseable, Visitable<MessageQueryIndex> {
    private static final int PER_SEGMENT_FILE_SIZE = 1024 * 1024 * 1024;

    private final LogManager logManager;
    private final CheckpointManager indexCheckpointManager;

    private final PeriodicFlushService indexCommittedCheckpointFlusher;

    private Map<CheckpointManager, CheckpointRecorder> iteratorCheckpointRecorderMap;

    public IndexLog(StorageConfig config, CheckpointManager indexCheckpointManager, List<CheckpointManager> iterateCheckpointManagers) {

        Preconditions.checkNotNull(iterateCheckpointManagers, "iterateCheckpointManagers should not be null!");

        this.logManager = new LogManager(new File(config.getIndexLogStorePath()), PER_SEGMENT_FILE_SIZE,
                new MaxSequenceLogSegmentValidator(indexCheckpointManager.getIndexCheckpointIndexOffset()));
        this.indexCheckpointManager = indexCheckpointManager;
        iteratorCheckpointRecorderMap = Maps.newHashMap();
        iterateCheckpointManagers.forEach(x -> {
            iteratorCheckpointRecorderMap.put(x, new CheckpointRecorder(x, x.getIndexIterateCheckpoint()));
        });

        this.indexCommittedCheckpointFlusher = new PeriodicFlushService(new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return 60 * 1000;
            }

            @Override
            public void flush() {
                for (CheckpointRecorder recorder : iteratorCheckpointRecorderMap.values()) {
                    long committed = recorder.committed.get();
                    recorder.checkpointManager.saveIndexIterateCheckpointSnapshot(new Snapshot<>(committed, committed));
                }
            }
        });
        indexCommittedCheckpointFlusher.start();
    }

    private static class CheckpointRecorder {
        private CheckpointManager checkpointManager;
        private AtomicLong committed;

        public CheckpointRecorder(CheckpointManager checkpointManager, long committed) {
            this.checkpointManager = checkpointManager;
            this.committed = new AtomicLong(committed);
        }
    }

    public void appendData(final long startOffset, final ByteBuf input) {
        if (!input.isReadable()) {
            indexCheckpointManager.updateMessageIndexCheckpoint(startOffset);
            return;
        }

        appendData(startOffset, input.nioBuffer());
    }

    private void appendData(final long startOffset, final ByteBuffer data) {
        final long indexOffset = indexCheckpointManager.getIndexCheckpointIndexOffset();
        LogSegment segment = logManager.locateSegment(indexOffset);
        if (segment == null) {
            segment = logManager.allocOrResetSegments(indexOffset);
        }

        final AppendMessageResult result = doAppendData(segment, data);
        switch (result.getStatus()) {
            case SUCCESS:
                indexCheckpointManager.updateIndexCheckpoint(startOffset, result.getWroteOffset());
                break;
            case END_OF_FILE:
                if (logManager.allocNextSegment() == null) {
                    return;
                }
                indexCheckpointManager.updateIndexLogCheckpoint(result.getWroteOffset());
                appendData(startOffset, data);
        }
    }

    private AppendMessageResult doAppendData(final LogSegment segment, final ByteBuffer data) {
        int currentPos = segment.getWrotePosition();
        final int freeSize = segment.getFileSize() - currentPos;
        if (data.remaining() <= freeSize) {
            if (!segment.appendData(data)) throw new RuntimeException("append index data failed.");
            return new AppendMessageResult<>(SUCCESS, segment.getBaseOffset() + segment.getWrotePosition());
        }

        ByteBuf to = ByteBufAllocator.DEFAULT.ioBuffer(freeSize);
        try {
            partialCopy(data, to);
            if (to.isWritable(Long.BYTES)) {
                to.writeLong(-1);
            }
            fillZero(to);
            if (!segment.appendData(to.nioBuffer())) throw new RuntimeException("append index data failed.");
        } finally {
            ReferenceCountUtil.release(to);
        }
        return new AppendMessageResult(END_OF_FILE, segment.getBaseOffset() + segment.getFileSize());
    }

    private void fillZero(ByteBuf buffer) {
        while (buffer.isWritable(Byte.BYTES)) {
            buffer.writeByte(0);
        }
    }

    private void partialCopy(ByteBuffer src, ByteBuf to) {
        while (to.isWritable(Long.BYTES)) {
            src.mark();
            to.markWriterIndex();
            if (!to.isWritable(Long.BYTES)) break;
            to.writeLong(src.getLong());

            if (!to.isWritable(Long.BYTES)) {
                src.reset();
                to.resetWriterIndex();
                break;
            }
            to.writeLong(src.getLong());

            // subject
            if (!writeString(src, to)) break;

            // msgId
            if (!writeString(src, to)) break;
        }
    }

    private boolean writeString(ByteBuffer src, ByteBuf to) {
        short len = src.getShort();
        if (!to.isWritable(Short.BYTES + len)) {
            src.reset();
            to.resetWriterIndex();
            return false;
        }
        if (len == 0) {
            to.writeShort(len);
            return true;
        }
        byte[] subject = new byte[len];
        src.get(subject);
        to.writeShort(len);
        to.writeBytes(subject);
        return true;
    }

    public IndexLogVisitor newVisitor(final long start) {
        return new IndexLogVisitor(logManager, start);
    }

    @Override
    public long getMinOffset() {
        return logManager.getMinOffset();
    }

    @Override
    public long getMaxOffset() {
        return logManager.getMaxOffset();
    }

    public long getMessageOffset() {
        return indexCheckpointManager.getIndexCheckpointMessageOffset();
    }

    public void clean() {

        long minCommitted = Long.MAX_VALUE;

        for (CheckpointRecorder recorder : iteratorCheckpointRecorderMap.values()) {
            long committed = recorder.committed.get();
            if (committed < minCommitted) {
                minCommitted = committed;
            }
        }

        logManager.deleteSegmentsBeforeOffset(minCommitted);
    }

    public void commit(CheckpointManager iterateCheckpointManager, long offset) {

        CheckpointRecorder checkpointRecorder = iteratorCheckpointRecorderMap.get(iterateCheckpointManager);
        if (checkpointRecorder == null) {
            throw new RuntimeException("iterateCheckpointManager not exist!");
        }

        long current = checkpointRecorder.committed.get();
        if (current < offset) {
            checkpointRecorder.committed.compareAndSet(current, offset);
        }
    }

    public void flush() {
        long currentTime = System.currentTimeMillis();
        final Snapshot<IndexCheckpoint> snapshot = indexCheckpointManager.createIndexCheckpoint();
        try {
            logManager.flush();
        } finally {
            Metrics.timer("Store.IndexLog.FlushTimer").update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
        indexCheckpointManager.saveIndexCheckpointSnapshot(snapshot);
    }

    @Override
    public void close() {
        logManager.close();
    }
}
