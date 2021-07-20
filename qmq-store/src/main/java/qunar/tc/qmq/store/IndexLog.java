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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.metrics.Metrics;

import static qunar.tc.qmq.store.AppendMessageStatus.*;

public class IndexLog implements AutoCloseable, Visitable<MessageQueryIndex> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexLog.class);

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
        int remaining = data.remaining();

        ByteBuf to = ByteBufAllocator.DEFAULT.ioBuffer(Math.min(remaining, freeSize));
        try {
            AppendMessageStatus status = copy(data, to);
            if (status == DATA_OVERFLOW) {
                fillZero(to);
            }
            if (!segment.appendData(to.nioBuffer())) throw new RuntimeException("append index data failed.");

            if (status == DATA_OVERFLOW) {
                return new AppendMessageResult(END_OF_FILE, segment.getBaseOffset() + segment.getFileSize());
            } else {
                return new AppendMessageResult(SUCCESS, segment.getBaseOffset() + segment.getWrotePosition());
            }
        } finally {
            ReferenceCountUtil.release(to);
        }
    }

    private void fillZero(ByteBuf buffer) {
        while (buffer.isWritable(Byte.BYTES)) {
            buffer.writeByte(0);
        }
    }

    private AppendMessageStatus copy(ByteBuffer src, ByteBuf to) {
        while (to.isWritable()) {
            src.mark();
            to.markWriterIndex();
            if (!to.isWritable(Long.BYTES)) {
                return DATA_OVERFLOW;
            }
            if (src.remaining() < Long.BYTES) {
                return SUCCESS;
            }
            long sequence = src.getLong();
            if (sequence <= 0) {
                LOGGER.warn("sequence <= 0");
            }
            to.writeLong(sequence);

            if (src.remaining() < Long.BYTES) {
                src.reset();
                to.resetWriterIndex();
                return SUCCESS;
            }

            if (!to.isWritable(Long.BYTES)) {
                src.reset();
                to.resetWriterIndex();
                return AppendMessageStatus.DATA_OVERFLOW;
            }
            long createTime = src.getLong();
            if (createTime <= 0) {
                LOGGER.warn("createTime <= 0");
            }
            to.writeLong(createTime);

            // subject
            AppendMessageStatus status = writeString(src, to);
            if (status == END_OF_FILE) {
                return SUCCESS;
            }
            if (status != SUCCESS) {
                return status;
            }

            // msgId
            status = writeString(src, to);
            if (status == END_OF_FILE) {
                return SUCCESS;
            }
            if (status != SUCCESS) {
                return status;
            }
            if (src.remaining() <= 0) {
                return SUCCESS;
            }
        }
        return DATA_OVERFLOW;
    }

    private AppendMessageStatus writeString(ByteBuffer src, ByteBuf to) {
        if (src.remaining() < Short.BYTES) {
            to.resetWriterIndex();
            return END_OF_FILE;
        }
        short len = src.getShort();
        if (len <= 0) {
            to.resetWriterIndex();
            return END_OF_FILE;
        }
        if (src.remaining() < len) {
            to.resetWriterIndex();
            return END_OF_FILE;
        }
        if (!to.isWritable(Short.BYTES + len)) {
            src.reset();
            to.resetWriterIndex();
            return DATA_OVERFLOW;
        }
        byte[] subject = new byte[len];
        src.get(subject);
        to.writeShort(len);
        to.writeBytes(subject);
        return SUCCESS;
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
