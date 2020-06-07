package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.result.Result;
import qunar.tc.qmq.utils.Checksums;
import qunar.tc.qmq.utils.Crc32;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
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

    private final File dir;
    private final int dataSize;

    private ConcurrentSkipListMap<PullLogSequence, SegmentLocation> index = new ConcurrentSkipListMap<>();

    private ValidateResult validateResult;

    public SortedPullLogTable(final File dir, final int dataSize) {
        this.dir = dir;
        this.dataSize = dataSize;
        loadAndValidateLogs();
    }

    /**
     * 启动的时候首先会加载磁盘上的文件，然后使用crc进行校验
     * 校验完毕后开始加载每个文件上的index部分信息，将这些信息放到内存的跳表里，供后面查询使用
     */
    private void loadAndValidateLogs() {
        LOG.info("Loading logs.");
        final TreeMap<Long, VarLogSegment> segments = new TreeMap<>();
        try {
            final File[] files = dir.listFiles();
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
        } finally {
            LOG.info("Load logs done.");
        }
        try {
            for (Map.Entry<Long, VarLogSegment> entry : segments.entrySet()) {
                final VarLogSegment segment = entry.getValue();
                final int positionOfIndex = TABLET_HEADER_SIZE + dataSize;
                ByteBuffer header = segment.selectBuffer(0, TABLET_HEADER_SIZE);
                header.position(POSITION_OF_TOTAL_PAYLOAD_SIZE);
                final int totalPayloadSize = header.getInt();

                if (!validate(totalPayloadSize, segment)) {
                    validateResult = new ValidateResult(LogSegmentValidator.ValidateStatus.PARTIAL, segment.getBaseOffset());
                    LOG.error("validate sorted pull log table failed: {}", segment);
                    break;
                }

                ByteBuffer buffer = segment.selectBuffer(positionOfIndex, totalPayloadSize - positionOfIndex);
                while (buffer.remaining() > 0) {
                    final short len = buffer.getShort();
                    final byte[] bytes = new byte[len];
                    buffer.get(bytes);
                    final String consumer = new String(bytes, StandardCharsets.UTF_8);
                    final long startOfPullLogSequence = buffer.getLong();
                    final long baseOfMessageSequence = buffer.getLong();
                    final int position = buffer.getInt();
                    index.put(new PullLogSequence(consumer, startOfPullLogSequence), new SegmentLocation(baseOfMessageSequence, position, segment));
                }

                validateResult = new ValidateResult(LogSegmentValidator.ValidateStatus.COMPLETE, segment.getBaseOffset());
            }
        } finally {
            LOG.info("Load logs done.");
        }
    }

    private boolean validate(int totalPayloadSize, VarLogSegment segment) {
        ByteBuffer buffer = segment.selectBuffer(totalPayloadSize, TABLET_CRC_SIZE);
        long crc = buffer.getLong();

        ByteBuffer payload = segment.selectBuffer(TABLET_HEADER_SIZE, totalPayloadSize);
        long computedCrc = Crc32.crc32(payload, 0, totalPayloadSize);
        return crc == computedCrc;
    }

    public Optional<TabletBuilder> newTabletBuilder(final long tabletId) throws IOException {
        return Optional.of(new TabletBuilder(this, new VarLogSegment(new File(dir, String.valueOf(tabletId)))));
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

        SegmentLocation location = entry.getValue();
        VarLogSegment logSegment = location.logSegment;
        PullLogSequence pullLogSequence = entry.getKey();
        long offset = pullSequence - pullLogSequence.startPullLogSequence;
        if (offset < 0) {
            return -1;
        }

        int position = TABLET_HEADER_SIZE + location.position + (int) offset;
        ByteBuffer buffer = logSegment.selectBuffer(position, PullLogMemTable.ENTRY_SIZE);
        if (buffer == null) return -1;
        int offsetOfMessageSequence = buffer.getInt();
        return location.baseOfMessageSequence + offsetOfMessageSequence;
    }

    @Override
    public void close() {
    }

    public ValidateResult getValidateResult() {
        return validateResult;
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
            final ByteBuffer buffer = ByteBuffer.allocate(TABLET_HEADER_SIZE);
            buffer.putInt(MagicCode.SORTED_MESSAGES_TABLE_MAGIC_V1);
            buffer.position(buffer.position() + Integer.BYTES);
            buffer.putLong(beginOffset);
            buffer.putLong(endOffset);
            buffer.flip();

            return tablet.appendData(buffer);
        }

        public Result<TabletBuilder.AppendStatus, Integer> append(final ByteBuffer buffer) {
            Checksums.update(crc, buffer, buffer.limit());
            final int position = tablet.getWrotePosition();
            if (tablet.appendData(buffer)) {
                return new Result<>(TabletBuilder.AppendStatus.SUCCESS, position);
            } else {
                return new Result<>(TabletBuilder.AppendStatus.ERROR, 0);
            }
        }

        public void appendIndex(Map<String, PullLogIndexEntry> indexMap) {
            for (Map.Entry<String, PullLogIndexEntry> entry : indexMap.entrySet()) {
                final byte[] consumerBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                int size = Short.BYTES + consumerBytes.length + Long.BYTES + Long.BYTES + Integer.BYTES;
                PullLogIndexEntry indexEntry = entry.getValue();

                ByteBuf buffer = ByteBufAllocator.DEFAULT.ioBuffer(size);
                try {
                    buffer.writeShort((short) consumerBytes.length);
                    buffer.writeBytes(consumerBytes);
                    buffer.writeLong(indexEntry.startOfPullLogSequence);
                    buffer.writeLong(indexEntry.baseOfMessageSequence);
                    buffer.writeLong(indexEntry.position);
                    ByteBuffer nioBuffer = buffer.nioBuffer();
                    Checksums.update(crc, nioBuffer, nioBuffer.limit());
                    tablet.appendData(nioBuffer);
                } finally {
                    ReferenceCountUtil.safeRelease(buffer);
                }

                ConcurrentSkipListMap<PullLogSequence, SegmentLocation> index = sortedPullLogTable.index;
                index.put(new PullLogSequence(entry.getKey(), indexEntry.startOfPullLogSequence), new SegmentLocation(indexEntry.baseOfMessageSequence, indexEntry.position, tablet));
            }
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
            tablet.flush();
        }

        public enum AppendStatus {
            SUCCESS,
            ERROR
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

    public static class ValidateResult {
        private final LogSegmentValidator.ValidateStatus validateStatus;

        private final long position;

        public ValidateResult(LogSegmentValidator.ValidateStatus validateStatus, long position) {
            this.validateStatus = validateStatus;
            this.position = position;
        }

        public LogSegmentValidator.ValidateStatus getValidateStatus() {
            return validateStatus;
        }

        public long getPosition() {
            return position;
        }
    }

}
