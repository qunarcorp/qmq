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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.utils.Checksums;
import qunar.tc.qmq.utils.Crc32;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by zhaohui.yu
 * 2020/6/7
 * <p>
 * 这里记录的pull log信息，将所有consumer的pull log混合在一个文件里记录，所以文件格式分为四个部分
 * header: 文件头，记录了magic code, 数据部分总大小，action log回放开始位置，action log回放结束位置
 * consumer log sequence: 这一部分记录的就是pull log里的内容，pull sequence对应位置的consumer log sequence，每个consumer的是连续的放在一块的
 * index: 因为所有consumer的pull log都放到一个文件里，则需要记录一些索引信息才能定位到。索引格式是:
 * consumer + pullLogSequence + base consumer log sequence(因为文件第二部分存储的都是int类型，所以这里记录一个base，这样可以缩小文件记录的长度)+position(该consumer记录块的开始位置)
 * crc: 最后对文件的数据内容(第二部分和第三部分)计算一个crc放在这里，启动的时候用于校验
 */
public class SortedPullLogTable implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SortedPullLogTable.class);

    // magic::<int> + payloadSize::<int>  + beginOffset::<long> + endOffset::<long>
    private final static int TABLET_HEADER_SIZE = Integer.BYTES * 2 + Long.BYTES * 2;
    private static final int TABLET_CRC_SIZE = Long.BYTES;

    private static final int TOTAL_PAYLOAD_SIZE_LEN = Integer.BYTES;
    private static final int POSITION_OF_TOTAL_PAYLOAD_SIZE = Integer.BYTES;

    private final File logDir;
    private final int dataSize;

    private ConcurrentSkipListMap<PullLogSequence, SegmentLocation> index = new ConcurrentSkipListMap<>();

    private volatile long actionReplayState;

    public SortedPullLogTable(final File logDir, final int dataSize) {
        this.logDir = logDir;
        this.dataSize = dataSize;
        createAndValidateLogDir();
        loadAndValidateLogs();
    }

    /**
     * 启动的时候首先会加载磁盘上的文件，然后使用crc进行校验
     * 校验完毕后开始加载每个文件上的index部分信息，将这些信息放到内存的跳表里，供后面查询使用
     */
    private void loadAndValidateLogs() {
        LOG.info("Loading logs.");
        final TreeMap<Long, VarLogSegment> segments = new TreeMap<>();
        final File[] files = logDir.listFiles();
        if (files == null) return;

        for (final File file : files) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            try {
                final VarLogSegment segment = new VarLogSegment(file);
                segments.put(segment.getBaseOffset(), segment);
                LOG.info("Load {} success.", file.getAbsolutePath());
            } catch (IOException e) {
                LOG.error("Load {} failed.", file.getAbsolutePath());
            }
        }
        try {
            FileHeader lastSegmentHeader = null;
            for (Map.Entry<Long, VarLogSegment> entry : segments.entrySet()) {
                final VarLogSegment segment = entry.getValue();
                Optional<FileHeader> validateResult = validate(lastSegmentHeader, segment);
                FileHeader currentHeader = null;
                if (validateResult.isPresent()) {
                    currentHeader = validateResult.get();
                    lastSegmentHeader = validateResult.get();
                } else if (lastSegmentHeader == null) {
                    LOG.error("validate pull log sorted table failed, no valid segments");
                    actionReplayState = 0;
                    break;
                } else {
                    actionReplayState = lastSegmentHeader.endOffset + 1;
                    LOG.error("validate pull log sorted table failed, in validate segment: {}", segment);
                    break;
                }

                final int positionOfIndex = TABLET_HEADER_SIZE + dataSize;
                final int totalPayloadSize = currentHeader.totalPayloadSize;
                ByteBuffer indexBuffer = segment.selectBuffer(positionOfIndex, totalPayloadSize - positionOfIndex);
                if (indexBuffer == null) {
                    actionReplayState = segment.getBaseOffset();
                    LOG.error("validate sorted pull log table failed: {}", segment);
                    break;
                }
                while (indexBuffer.remaining() > 0) {
                    indexBuffer.getInt();
                    final short len = indexBuffer.getShort();
                    final byte[] bytes = new byte[len];
                    indexBuffer.get(bytes);
                    final String consumer = new String(bytes, StandardCharsets.UTF_8);
                    final long startOfPullLogSequence = indexBuffer.getLong();
                    final long baseOfMessageSequence = indexBuffer.getLong();
                    final int position = indexBuffer.getInt();
                    segment.retain();
                    index.put(new PullLogSequence(consumer, startOfPullLogSequence), new SegmentLocation(baseOfMessageSequence, position, segment));
                }

                actionReplayState = currentHeader.endOffset + 1;
            }
        } finally {
            LOG.info("Validate logs done.");
        }
    }

    private Optional<FileHeader> validate(FileHeader lastFileHeader, VarLogSegment segment) {
        long fileSize = segment.getFileSize();
        long minFieSize = TABLET_CRC_SIZE + TABLET_HEADER_SIZE;
        if (fileSize <= minFieSize) Optional.empty();

        ByteBuffer header = segment.selectBuffer(0, TABLET_HEADER_SIZE);
        if (header == null) return Optional.empty();
        int magicCode = header.getInt();
        if (magicCode != MagicCode.SORTED_MESSAGES_TABLE_MAGIC_V1) return Optional.empty();
        int totalPayloadSize = header.getInt();
        if (fileSize != TABLET_HEADER_SIZE + totalPayloadSize + TABLET_CRC_SIZE) return Optional.empty();
        long beginOffset = header.getLong();
        long endOffset = header.getLong();

        if (lastFileHeader != null && (lastFileHeader.endOffset + 1) != beginOffset) return Optional.empty();

        ByteBuffer buffer = segment.selectBuffer(totalPayloadSize + TABLET_HEADER_SIZE, TABLET_CRC_SIZE);
        if (buffer == null) return Optional.empty();
        long crc = buffer.getLong();

        ByteBuffer payload = segment.selectBuffer(TABLET_HEADER_SIZE, totalPayloadSize);
        if (payload == null) return Optional.empty();
        long computedCrc = Crc32.crc32(payload, 0, totalPayloadSize);
        return crc == computedCrc ? Optional.of(new FileHeader(totalPayloadSize, beginOffset, endOffset)) : Optional.empty();
    }

    public Optional<TabletBuilder> newTabletBuilder(final long tabletId) throws IOException {
        return Optional.of(new TabletBuilder(this, new VarLogSegment(new File(logDir, String.valueOf(tabletId)))));
    }

    /**
     * 查询的时候，首先要找到对应consumer在文件块的开始位置
     *
     * @param subject
     * @param group
     * @param consumerId
     * @param pullSequence
     * @return
     */
    public long getMessage(String subject, String group, String consumerId, long pullSequence) {
        String consumer = PullLogMemTable.keyOf(subject, group, consumerId);
        Map.Entry<PullLogSequence, SegmentLocation> entry = index.lowerEntry(new PullLogSequence(consumer, pullSequence));
        if (entry == null) {
            return -1;
        }

        if (!entry.getKey().consumer.equals(consumer)) {
            return -1;
        }

        final SegmentLocation location = entry.getValue();
        final VarLogSegment logSegment = location.logSegment;
        final PullLogSequence pullLogSequence = entry.getKey();
        long offset = pullSequence - pullLogSequence.startPullLogSequence;
        if (offset < 0) {
            return -1;
        }

        final int position = TABLET_HEADER_SIZE + location.position + (int) offset;
        final ByteBuffer buffer = logSegment.selectBuffer(position, PullLogMemTable.ENTRY_SIZE);
        if (buffer == null) return -1;
        final int offsetOfMessageSequence = buffer.getInt();
        return location.baseOfMessageSequence + offsetOfMessageSequence;
    }

    private void createAndValidateLogDir() {
        if (!logDir.exists()) {
            LOG.info("Log directory {} not found, try create it.", logDir.getAbsoluteFile());
            try {
                Files.createDirectories(logDir.toPath());
            } catch (InvalidPathException e) {
                LOG.error("log directory char array: {}", Arrays.toString(logDir.getAbsolutePath().toCharArray()));
                throw new RuntimeException("Failed to create log directory " + logDir.getAbsolutePath(), e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create log directory " + logDir.getAbsolutePath(), e);
            }
        }

        if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new RuntimeException(logDir.getAbsolutePath() + " is not a readable log directory");
        }
    }

    /**
     * 根据每个consumer的ack进度来删除pull log table的segment文件
     * 每个segment有个引用计数，计数为0的时候可以删除
     * <p>
     * 寻找可以删除的LogSegment: 用ack进度在index跳表里查lowerEntry，再以这个lowerEntry查下一个lowerEntry，如果这个存在的话则说明这一段的pull sequence都可以不保留了
     * 这个时候就将对应LogSegment的引用计数减一，减为0的时候就可以删了
     *
     * @param progresses
     */
    public void clean(Collection<ConsumerGroupProgress> progresses) {
        for (ConsumerGroupProgress progress : progresses) {
            if (progress.isBroadcast()) continue;
            Map<String, ConsumerProgress> consumerProgress = progress.getConsumers();
            for (Map.Entry<String, ConsumerProgress> entry : consumerProgress.entrySet()) {
                PullLogSequence ackSequence = new PullLogSequence(PullLogMemTable.keyOf(progress.getSubject(), progress.getGroup(), entry.getKey()), entry.getValue().getAck());
                Map.Entry<PullLogSequence, SegmentLocation> lowerEntry = index.lowerEntry(ackSequence);
                if (!isSameConsumer(lowerEntry, ackSequence.consumer)) continue;

                Map.Entry<PullLogSequence, SegmentLocation> lowerLowerEntry = index.lowerEntry(lowerEntry.getKey());

                recursionDelete(lowerLowerEntry, ackSequence.consumer);
            }
        }
    }


    /**
     * 递归删除
     *
     * @param start
     * @param consumer
     */
    private void recursionDelete(Map.Entry<PullLogSequence, SegmentLocation> start, String consumer) {
        if (!isSameConsumer(start, consumer)) return;
        SegmentLocation location = index.remove(start.getKey());
        if (location != null) {
            VarLogSegment logSegment = location.logSegment;
            logSegment.release();
            if (logSegment.disable()) {

                logSegment.destroy();
            }
        }
        Map.Entry<PullLogSequence, SegmentLocation> lowerEntry = index.lowerEntry(start.getKey());
        recursionDelete(lowerEntry, consumer);
    }

    private boolean isSameConsumer(Map.Entry<PullLogSequence, SegmentLocation> entry, String consumer) {
        if (entry == null) return false;
        return entry.getKey().consumer.equals(consumer);
    }

    @Override
    public void close() {
    }

    public long getActionReplayState() {
        return actionReplayState;
    }

    public static final class TabletBuilder {
        private final SortedPullLogTable sortedPullLogTable;
        private final VarLogSegment tablet;
        private final Crc32 crc;

        private TabletBuilder(final SortedPullLogTable sortedPullLogTable, final VarLogSegment tablet) {
            this.sortedPullLogTable = sortedPullLogTable;
            this.tablet = tablet;
            this.crc = new Crc32();
        }

        public long getTabletId() {
            return tablet.getBaseOffset();
        }

        public boolean begin(final long beginOffset, final long endOffset) {
            tablet.retain();
            final ByteBuffer buffer = ByteBuffer.allocate(TABLET_HEADER_SIZE);
            buffer.putInt(MagicCode.SORTED_MESSAGES_TABLE_MAGIC_V1);
            buffer.position(buffer.position() + Integer.BYTES);
            buffer.putLong(beginOffset);
            buffer.putLong(endOffset);
            buffer.flip();

            return tablet.appendData(buffer);
        }

        public boolean append(final ByteBuffer buffer) {
            Checksums.update(crc, buffer, buffer.limit());
            return tablet.appendData(buffer);
        }

        public boolean appendIndex(Map<String, PullLogIndexEntry> indexMap) {
            for (Map.Entry<String, PullLogIndexEntry> entry : indexMap.entrySet()) {
                final byte[] consumerBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                int size = Integer.BYTES + Short.BYTES + consumerBytes.length + Long.BYTES + Long.BYTES + Integer.BYTES;
                PullLogIndexEntry indexEntry = entry.getValue();

                ByteBuf buffer = ByteBufAllocator.DEFAULT.ioBuffer(size);
                try {
                    buffer.writeInt(MagicCode.PULL_LOG_MAGIC_V1);
                    buffer.writeShort((short) consumerBytes.length);
                    buffer.writeBytes(consumerBytes);
                    buffer.writeLong(indexEntry.startOfPullLogSequence);
                    buffer.writeLong(indexEntry.baseOfMessageSequence);
                    buffer.writeLong(indexEntry.position);
                    ByteBuffer nioBuffer = buffer.nioBuffer();
                    Checksums.update(crc, nioBuffer, nioBuffer.limit());
                    boolean result = tablet.appendData(nioBuffer);
                    if (!result) return false;
                } finally {
                    ReferenceCountUtil.safeRelease(buffer);
                }

                ConcurrentSkipListMap<PullLogSequence, SegmentLocation> index = sortedPullLogTable.index;
                tablet.retain();
                index.put(new PullLogSequence(entry.getKey(), indexEntry.startOfPullLogSequence), new SegmentLocation(indexEntry.baseOfMessageSequence, indexEntry.position, tablet));
            }
            return true;
        }

        public boolean finish() {
            setTotalPayloadSize();
            final ByteBuffer buffer = ByteBuffer.allocate(TABLET_CRC_SIZE);
            buffer.putLong(crc.getValue());
            buffer.flip();

            final boolean ok = tablet.appendData(buffer);
            if (ok) {
                tablet.setWrotePosition(tablet.getWrotePosition());
            }
            tablet.release();
            return ok;
        }

        private void setTotalPayloadSize() {
            int current = tablet.getWrotePosition();
            int totalPayloadSize = current - TABLET_HEADER_SIZE;
            tablet.setWrotePosition(POSITION_OF_TOTAL_PAYLOAD_SIZE);
            final ByteBuffer totalPayloadSizeBuffer = ByteBuffer.allocate(TOTAL_PAYLOAD_SIZE_LEN);
            totalPayloadSizeBuffer.putInt(totalPayloadSize);
            totalPayloadSizeBuffer.flip();
            tablet.appendData(totalPayloadSizeBuffer);
            tablet.setWrotePosition(current);
        }

        public void flush() {
            //有可能在文件要flush的时候，消费进度已经全部超过了这个文件里的pull sequence，这样就不用flush了，直接将文件删除掉
            if (tablet.disable()) {
                tablet.destroy();
                return;
            }
            tablet.flush();
        }
    }

    private static class PullLogSequence implements Comparable<PullLogSequence> {
        private final String consumer;

        private final long startPullLogSequence;

        public PullLogSequence(String consumer, long startPullLogSequence) {
            this.consumer = consumer;
            this.startPullLogSequence = startPullLogSequence;
        }

        @Override
        public int compareTo(PullLogSequence o) {
            if (consumer.equals(o.consumer)) {
                return Long.compare(startPullLogSequence, o.startPullLogSequence);
            }
            return consumer.compareTo(o.consumer);
        }
    }

    private static class SegmentLocation {
        private final long baseOfMessageSequence;

        private final int position;

        private final VarLogSegment logSegment;

        public SegmentLocation(long baseOfMessageSequence, int position, VarLogSegment logSegment) {
            this.baseOfMessageSequence = baseOfMessageSequence;

            this.position = position;
            this.logSegment = logSegment;
        }
    }

    private static class FileHeader {
        private final int totalPayloadSize;

        private final long beginOffset;

        private final long endOffset;

        private FileHeader(int totalPayloadSize, long beginOffset, long endOffset) {
            this.totalPayloadSize = totalPayloadSize;
            this.beginOffset = beginOffset;
            this.endOffset = endOffset;
        }
    }

}
