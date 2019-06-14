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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import qunar.tc.qmq.metrics.Metrics;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.store.AppendMessageStatus.END_OF_FILE;
import static qunar.tc.qmq.store.AppendMessageStatus.SUCCESS;

public class IndexLog implements AutoCloseable {
    private static final int PER_SEGMENT_FILE_SIZE = 1024 * 1024 * 1024;

    private final LogManager logManager;
    private final CheckpointManager checkpointManager;

    private volatile long deleteTo;

    public IndexLog(StorageConfig config, CheckpointManager checkpointManager) {
        this.logManager = new LogManager(new File(config.getIndexLogStorePath()), PER_SEGMENT_FILE_SIZE,
                new MaxSequenceLogSegmentValidator(checkpointManager.getIndexCheckpointIndexOffset()));
        this.checkpointManager = checkpointManager;
    }

    public void appendData(final long startOffset, final ByteBuf input) {
        if (!input.isReadable()) {
            checkpointManager.updateMessageIndexCheckpoint(startOffset);
            return;
        }

        appendData(startOffset, input.nioBuffer());
    }

    private void appendData(final long startOffset, final ByteBuffer data) {
        final long indexOffset = checkpointManager.getIndexCheckpointIndexOffset();
        LogSegment segment = logManager.locateSegment(indexOffset);
        if (segment == null) {
            segment = logManager.allocOrResetSegments(indexOffset);
        }

        final AppendMessageResult result = doAppendData(segment, data);
        switch (result.getStatus()) {
            case SUCCESS:
                checkpointManager.updateIndexCheckpoint(startOffset, result.getWroteOffset());
                break;
            case END_OF_FILE:
                if (logManager.allocNextSegment() == null) {
                    return;
                }
                checkpointManager.updateIndexLogCheckpoint(result.getWroteOffset());
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
        byte[] subject = new byte[len];
        src.get(subject);
        to.writeShort(len);
        to.writeBytes(subject);
        return true;
    }

    public IndexLogVisitor newVisitor(final long start) {
        return new IndexLogVisitor(logManager, start);
    }

    public long getMessageOffset() {
        return checkpointManager.getIndexCheckpointMessageOffset();
    }

    public void clean() {
        logManager.deleteSegmentsBeforeOffset(deleteTo);
    }

    public void setDeleteTo(long deleteTo) {
        if (this.deleteTo < deleteTo) this.deleteTo = deleteTo;
    }

    public long maxOffset() {
        return logManager.getMaxOffset();
    }

    public long minOffset() {
        return logManager.getMinOffset();
    }

    public void flush() {
        long currentTime = System.currentTimeMillis();
        final Snapshot<IndexCheckpoint> snapshot = checkpointManager.createIndexCheckpoint();
        try {
            logManager.flush();
        } finally {
            Metrics.timer("Store.IndexLog.FlushTimer").update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
        checkpointManager.saveIndexCheckpointSnapshot(snapshot);
    }

    @Override
    public void close() {
        logManager.close();
    }
}
