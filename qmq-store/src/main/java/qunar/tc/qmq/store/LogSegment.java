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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author keli.wang
 * @since 2017/7/3
 */
public class LogSegment extends ReferenceObject {
    private static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

    private final File file;
    private final int fileSize;
    private final String fileName;
    private final long baseOffset;

    private final AtomicInteger wrotePosition = new AtomicInteger(0);

    private RandomAccessFile rawFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    public LogSegment(final File file, final int fileSize) throws IOException {
        this.file = file;
        this.fileSize = fileSize;
        this.fileName = file.getAbsolutePath();
        this.baseOffset = Long.parseLong(file.getName());

        boolean success = false;
        try {
            rawFile = new RandomAccessFile(file, "rw");
            fileChannel = rawFile.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            success = true;
        } catch (FileNotFoundException e) {
            LOG.error("create file channel failed. file: {}", fileName, e);
            throw e;
        } catch (IOException e) {
            LOG.error("map file failed. file: {}", fileName, e);
            throw e;
        } finally {
            if (!success && fileChannel != null) {
                fileChannel.close();
            }
        }
    }

    public <T, R> AppendMessageResult<R> append(final T message, final MessageAppender<T, R> appender) {
        final int currentPos = wrotePosition.get();
        if (currentPos > fileSize) {
            return new AppendMessageResult<>(AppendMessageStatus.UNKNOWN_ERROR);
        }
        if (currentPos == fileSize) {
            return new AppendMessageResult<>(AppendMessageStatus.END_OF_FILE);
        }

        final ByteBuffer buffer = mappedByteBuffer.slice();
        buffer.position(currentPos);
        final AppendMessageResult<R> result = appender.doAppend(getBaseOffset(), buffer, fileSize - currentPos, message);
        this.wrotePosition.addAndGet(result.getWroteBytes());
        return result;
    }

    public boolean appendData(final ByteBuffer data) {
        final int currentPos = wrotePosition.get();
        final int size = data.limit();
        if (currentPos + size > fileSize) {
            return false;
        }

        try {
            fileChannel.write(data, currentPos);
            this.wrotePosition.addAndGet(size);
        } catch (Throwable e) {
            LOG.error("Append data to log segment failed.", e);
            return false;
        }
        return true;
    }

    public void fillPreBlank(final ByteBuffer blank, final long untilWhere) {
        final int currentPos = wrotePosition.get();
        final int untilPos = (int) (untilWhere % fileSize);
        final int size = untilPos - currentPos;
        if (size <= 0) return;

        final ByteBuffer buffer = mappedByteBuffer.slice();
        buffer.position(currentPos);
        buffer.limit(untilPos);
        buffer.put(blank);
        this.wrotePosition.addAndGet(size);
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int position) {
        wrotePosition.set(position);
    }

    public int getFileSize() {
        return fileSize;
    }

    public long getLastModifiedTime() {
        return file.lastModified();
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public boolean isFull() {
        return wrotePosition.get() == fileSize;
    }

    public ByteBuffer sliceByteBuffer() {
        return mappedByteBuffer.slice();
    }

    public SegmentBuffer selectSegmentBuffer(final int pos) {
        final int wrotePosition = getWrotePosition();
        if (pos < wrotePosition && pos >= 0) {

            final ByteBuffer buffer = mappedByteBuffer.slice();
            buffer.position(pos);

            final ByteBuffer bufferNew = buffer.slice();
            final int size = wrotePosition - pos;
            bufferNew.limit(size);
            return new SegmentBuffer(getBaseOffset() + pos, bufferNew, size, this);
        }

        return null;
    }

    public SegmentBuffer selectSegmentBuffer(final int pos, final int size) {
        final int wrotePosition = getWrotePosition();
        if ((pos + size) > wrotePosition) {
            return null;
        }

        final ByteBuffer buffer = mappedByteBuffer.slice();
        buffer.position(pos);

        final ByteBuffer bufferNew = buffer.slice();
        bufferNew.limit(size);
        return new SegmentBuffer(getBaseOffset() + pos, bufferNew, size, this);
    }

    public int flush() {
        final int value = wrotePosition.get();
        try {
            if (fileChannel.position() != 0) {
                fileChannel.force(false);
            } else {
                mappedByteBuffer.force();
            }
        } catch (IOException e) {
            throw new RuntimeException("flush segment failed. segment: " + fileName, e);
        }

        return value;
    }

    public void close() {
        try {
            fileChannel.close();
            clean(mappedByteBuffer);
        } catch (Exception e) {
            LOG.error("close file channel failed. file: {}", fileName, e);
        }
    }

    // MappedByteBuffer 是不会随着 channel 的关闭而释放的，需要通过特殊方法调用 clean 来释放
    private void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }

        final Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        cleaner.clean();
    }

    public boolean destroy() {
        close();
        return file.delete();
    }

    @Override
    public String toString() {
        return "LogSegment{" +
                "file=" + file.getAbsolutePath() +
                '}';
    }

}
