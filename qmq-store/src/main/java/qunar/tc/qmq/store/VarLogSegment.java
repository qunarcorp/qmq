package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaohui.yu
 * 2020/6/7
 * <p>
 * 变长log，也就是一个segment的长度是不固定的
 */
public class VarLogSegment extends ReferenceObject {
    private static final Logger LOG = LoggerFactory.getLogger(VarLogSegment.class);

    private final File file;
    private final String fileName;
    private final long baseOffset;

    private final AtomicInteger wrotePosition = new AtomicInteger(0);

    private FileChannel fileChannel;

    public VarLogSegment(final File file) throws IOException {
        this.file = file;
        this.fileName = file.getAbsolutePath();
        this.baseOffset = Long.parseLong(file.getName());

        boolean success = false;
        try {
            final RandomAccessFile rawFile = new RandomAccessFile(file, "rw");
            fileChannel = rawFile.getChannel();
            success = true;
        } catch (FileNotFoundException e) {
            LOG.error("create file channel failed. file: {}", fileName, e);
            throw e;
        } finally {
            if (!success && fileChannel != null) {
                fileChannel.close();
            }
        }
    }

    public boolean appendData(final ByteBuffer data) {
        final int currentPos = wrotePosition.get();
        final int size = data.limit();

        try {
            fileChannel.write(data, currentPos);
            this.wrotePosition.addAndGet(size);
        } catch (Throwable e) {
            LOG.error("Append data to log segment failed.", e);
            return false;
        }
        return true;
    }

    public ByteBuffer selectBuffer(final int position, final int size) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            fileChannel.read(buffer, position);
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            LOG.error("Select buffer from segment failed.", e);
            return null;
        }
    }

    public long getFileSize() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            return -1;
        }
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int position) {
        wrotePosition.set(position);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public int flush() {
        final int value = wrotePosition.get();
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            throw new RuntimeException("flush segment failed. segment: " + fileName, e);
        }

        return value;
    }

    public void close() {
        try {
            fileChannel.close();
        } catch (Exception e) {
            LOG.error("close file channel failed. file: {}", fileName, e);
        }
    }

    public boolean destroy() {
        close();
        return file.delete();
    }

    @Override
    public String toString() {
        return "VarLogSegment{" +
                "file=" + file.getAbsolutePath() +
                '}';
    }
}
