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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.store.result.Result;
import qunar.tc.qmq.utils.Checksums;
import qunar.tc.qmq.utils.Crc32;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author keli.wang
 * @since 2019-06-10
 */
public class SortedMessagesTable implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SortedMessagesTable.class);

    // magic::<int> + payloadSize::<int>  + beginOffset::<long> + endOffset::<long>
    private final static int TABLET_HEADER_SIZE = Integer.BYTES * 2 + Long.BYTES * 2;
    private static final int TABLET_CRC_SIZE = Long.BYTES;
    private static final int TABLET_META_SIZE = TABLET_HEADER_SIZE + TABLET_CRC_SIZE;

    private final long tabletSize;
    private final LogManager logManager;

    public SortedMessagesTable(final File dir, final int tabletSize) {
        this.tabletSize = tabletSize;
        this.logManager = new LogManager(dir, tabletSize, new TabletValidator());
    }

    public int getTabletMetaSize() {
        return TABLET_META_SIZE;
    }

    public long getNextTabletId(final long tabletId) {
        if (tabletId >= 0) {
            return tabletId + tabletSize;
        }

        final LogSegment tablet = logManager.latestSegment();
        if (tablet == null) {
            return 0;
        }

        if (tablet.getWrotePosition() < tablet.getFileSize()) {
            return tablet.getBaseOffset();
        } else {
            return tablet.getBaseOffset() + tablet.getFileSize();
        }
    }

    public Optional<TabletBuilder> newTabletBuilder(final long tabletId) {
        return logManager.getOrAllocSegment(tabletId).map(tablet -> {
            tablet.setWrotePosition(0);
            return new TabletBuilder(tablet);
        });
    }

    public Result<GetMessageStatus, SegmentBuffer> getMessage(final long tabletId, final int position, final int size) {
        final LogSegment tablet = logManager.locateSegment(tabletId);
        if (tablet == null) {
            return new Result<>(GetMessageStatus.TABLET_NOT_FOUND, null);
        }

        if (tablet.getBaseOffset() != tabletId) {
            return new Result<>(GetMessageStatus.TABLET_ID_INVALID, null);
        }

        final SegmentBuffer result = tablet.selectSegmentBuffer(position, size);
        if (result == null) {
            return new Result<>(GetMessageStatus.MESSAGE_NOT_FOUND, null);
        }

        if (result.retain()) {
            return new Result<>(GetMessageStatus.SUCCESS, result);
        } else {
            return new Result<>(GetMessageStatus.TABLET_DELETED, null);
        }
    }

    @Override
    public void close() {
        logManager.close();
    }

    public void clean(final StorageConfig config, final LogManager.DeleteHook hook) {
        logManager.deleteExpiredSegments(config.getSMTRetentionMs(), hook);
    }

    public enum GetMessageStatus {
        SUCCESS,
        TABLET_ID_INVALID,
        TABLET_NOT_FOUND,
        TABLET_DELETED,
        MESSAGE_NOT_FOUND
    }

    public static final class TabletBuilder {
        private final LogSegment tablet;
        private final Crc32 crc;

        private TabletBuilder(final LogSegment tablet) {
            this.tablet = tablet;
            this.crc = new Crc32();
        }

        public long getTabletId() {
            return tablet.getBaseOffset();
        }

        public boolean begin(final int totalSize, final long beginOffset, final long endOffset) {
            final ByteBuffer buffer = ByteBuffer.allocate(TABLET_HEADER_SIZE);
            buffer.putInt(MagicCode.SORTED_MESSAGES_TABLE_MAGIC_V1);
            buffer.putInt(totalSize);
            buffer.putLong(beginOffset);
            buffer.putLong(endOffset);
            buffer.flip();

            return tablet.appendData(buffer);
        }

        public Result<AppendStatus, Integer> append(final ByteBuffer message) {
            Checksums.update(crc, message, message.limit());
            final int position = tablet.getWrotePosition();
            if (tablet.appendData(message)) {
                return new Result<>(AppendStatus.SUCCESS, position);
            } else {
                return new Result<>(AppendStatus.ERROR, 0);
            }
        }

        public boolean finish() {
            final ByteBuffer buffer = ByteBuffer.allocate(TABLET_CRC_SIZE);
            buffer.putLong(crc.getValue());
            buffer.flip();

            final boolean ok = tablet.appendData(buffer);
            if (ok) {
                tablet.setWrotePosition(tablet.getFileSize());
            }
            return ok;
        }

        public void flush() {
            tablet.flush();
        }

        public enum AppendStatus {
            SUCCESS,
            ERROR
        }
    }

    private static final class TabletValidator implements LogSegmentValidator {
        @Override
        public ValidateResult validate(final LogSegment segment) {
            final ByteBuffer buffer = segment.sliceByteBuffer();
            if (buffer.remaining() < TABLET_META_SIZE) {
                return new ValidateResult(ValidateStatus.PARTIAL, 0);
            }

            final int magic = buffer.getInt();
            if (magic != MagicCode.SORTED_MESSAGES_TABLE_MAGIC_V1) {
                return new ValidateResult(ValidateStatus.PARTIAL, 0);
            }

            final int totalSize = buffer.getInt();
            // skip begin offset and end offset
            buffer.getLong();
            buffer.getLong();

            if (buffer.remaining() < totalSize + Long.BYTES) {
                return new ValidateResult(ValidateStatus.PARTIAL, 0);
            }

            final long computedCrc = computeMessageCrc(buffer, totalSize);

            // skip all messages
            buffer.position(buffer.position() + totalSize);
            final long crc = buffer.getLong();

            if (computedCrc == crc) {
                return new ValidateResult(ValidateStatus.COMPLETE, segment.getFileSize());
            } else {
                LOG.error("tablet crc check failed. computed crc: {}, stored crc: {}", computedCrc, crc);
                return new ValidateResult(ValidateStatus.PARTIAL, 0);
            }
        }

        private long computeMessageCrc(final ByteBuffer buffer, final int totalSize) {
            final ByteBuffer slice = buffer.slice();
            slice.limit(totalSize);
            return Crc32.crc32(slice, 0, totalSize);
        }
    }
}