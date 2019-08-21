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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

/**
 * @author keli.wang
 * @since 2017/7/3
 */
public class LogManager {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    private final File logDir;
    private final int fileSize;

    private final LogSegmentValidator segmentValidator;
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();

    private long flushedOffset = 0;

    public LogManager(final File dir, final int fileSize, final LogSegmentValidator segmentValidator) {
        this.logDir = dir;
        this.fileSize = fileSize;
        this.segmentValidator = segmentValidator;
        createAndValidateLogDir();
        loadLogs();
        recover();
    }

    // ensure dir ok
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

    private void loadLogs() {
        LOG.info("Loading logs.");
        try {
            final File[] files = logDir.listFiles();
            if (files == null) return;

            for (final File file : files) {
                if (file.getName().startsWith(".")) {
                    continue;
                }
                try {
                    final LogSegment segment = new LogSegment(file, fileSize);
                    segment.setWrotePosition(fileSize);
                    segments.put(segment.getBaseOffset(), segment);
                    LOG.info("Load {} success.", file.getAbsolutePath());
                } catch (IOException e) {
                    LOG.error("Load {} failed.", file.getAbsolutePath());
                }
            }
        } finally {
            LOG.info("Load logs done.");
        }
    }

    private void recover() {
        if (segments.isEmpty()) {
            return;
        }

        LOG.info("Recovering logs.");
        final List<Long> baseOffsets = new ArrayList<>(segments.navigableKeySet());
        final int offsetCount = baseOffsets.size();
        long offset = -1;
        for (int i = offsetCount - 2; i < offsetCount; i++) {
            if (i < 0) continue;

            final LogSegment segment = segments.get(baseOffsets.get(i));
            offset = segment.getBaseOffset();
            final LogSegmentValidator.ValidateResult result = segmentValidator.validate(segment);
            offset += result.getValidatedSize();
            if (result.getStatus() == LogSegmentValidator.ValidateStatus.COMPLETE) {
                segment.setWrotePosition(segment.getFileSize());
            } else {
                break;
            }
        }
        flushedOffset = offset;

        final LogSegment latestSegment = latestSegment();
        final long maxOffset = latestSegment.getBaseOffset() + latestSegment.getFileSize();
        final int relativeOffset = (int) (offset % fileSize);
        final LogSegment segment = locateSegment(offset);
        if (segment != null && maxOffset != offset) {
            segment.setWrotePosition(relativeOffset);
            LOG.info("recover wrote offset to {}:{}", segment, segment.getWrotePosition());
            if (segment.getBaseOffset() != latestSegment.getBaseOffset()) {
                LOG.info("will remove all segment after max wrote position. current base offset: {}, max base offset: {}",
                        segment.getBaseOffset(), latestSegment.getBaseOffset());
                deleteSegmentsAfterOffset(offset);
            }
        }
        LOG.info("Recover done.");
    }

    public LogSegment locateSegment(final long offset) {
        if (isBaseOffset(offset)) {
            return segments.get(offset);
        }

        final Map.Entry<Long, LogSegment> entry = segments.lowerEntry(offset);
        if (entry == null) {
            return null;
        } else {
            return entry.getValue();
        }
    }

    private boolean isBaseOffset(final long offset) {
        return offset % fileSize == 0;
    }

    public LogSegment firstSegment() {
        final Map.Entry<Long, LogSegment> entry = segments.firstEntry();
        return entry == null ? null : entry.getValue();
    }

    public LogSegment latestSegment() {
        final Map.Entry<Long, LogSegment> entry = segments.lastEntry();
        return entry == null ? null : entry.getValue();
    }

    public LogSegment allocNextSegment() {
        final long nextBaseOffset = nextSegmentBaseOffset();
        return allocSegment(nextBaseOffset);
    }

    private long nextSegmentBaseOffset() {
        final LogSegment segment = latestSegment();
        if (segment == null) {
            return 0;
        } else {
            return segment.getBaseOffset() + fileSize;
        }
    }

    private LogSegment allocSegment(final long baseOffset) {
        final File nextSegmentFile = new File(logDir, StoreUtils.offset2FileName(baseOffset));
        try {
            final LogSegment segment = new LogSegment(nextSegmentFile, fileSize);
            segments.put(baseOffset, segment);
            LOG.info("alloc new segment file {}", segment);
            return segment;
        } catch (IOException e) {
            LOG.error("Failed create new segment file. file: {}", nextSegmentFile.getAbsolutePath());
        }
        return null;
    }

    public Optional<LogSegment> getOrAllocSegment(final long baseOffset) {
        if (!isBaseOffset(baseOffset)) {
            return Optional.empty();
        }

        final LogSegment segment = segments.get(baseOffset);
        if (segment != null) {
            return Optional.of(segment);
        }

        return Optional.ofNullable(allocSegment(baseOffset));
    }

    public LogSegment allocOrResetSegments(final long expectedOffset) {
        final long baseOffset = computeBaseOffset(expectedOffset);

        if (segments.isEmpty()) {
            return allocSegment(baseOffset);
        }

        if (nextSegmentBaseOffset() == baseOffset && latestSegment().isFull()) {
            return allocSegment(baseOffset);
        }

        LOG.warn("All segments are too old, need to delete all segment now. Current base offset: {}, expect base offset: {}",
                latestSegment().getBaseOffset(), baseOffset);
        deleteAllSegments();

        return allocSegment(baseOffset);
    }

    private long computeBaseOffset(final long offset) {
        return offset - (offset % fileSize);
    }

    private void deleteAllSegments() {
        for (Map.Entry<Long, LogSegment> entry : segments.entrySet()) {
            deleteSegment(entry.getKey(), entry.getValue());
        }
    }

    public long getMinOffset() {
        final LogSegment segment = firstSegment();
        if (segment == null) {
            return 0;
        }
        return segment.getBaseOffset();
    }

    public long getMaxOffset() {
        final LogSegment segment = latestSegment();
        if (segment == null) {
            return 0;
        }
        return segment.getBaseOffset() + segment.getWrotePosition();
    }

    public boolean flush() {
        ConcurrentNavigableMap<Long, LogSegment> beingFlushView = findBeingFlushView();
        int lastOffset = -1;
        long lastBaseOffset = -1;
        for (Map.Entry<Long, LogSegment> entry : beingFlushView.entrySet()) {
            try {
                LogSegment segment = entry.getValue();
                lastOffset = segment.flush();
                lastBaseOffset = segment.getBaseOffset();
            } catch (Exception e) {
                break;
            }
        }

        if (lastBaseOffset == -1 || lastOffset == -1) return false;
        final long where = lastBaseOffset + lastOffset;
        boolean result = where != this.flushedOffset;
        this.flushedOffset = where;
        return result;
    }

    private ConcurrentNavigableMap<Long, LogSegment> findBeingFlushView() {
        LogSegment lastFlush = locateSegment(flushedOffset);
        if (lastFlush == null) {
            return segments;
        }
        return segments.tailMap(lastFlush.getBaseOffset(), true);
    }

    public void close() {
        for (final LogSegment segment : segments.values()) {
            segment.close();
        }
    }

    public void deleteExpiredSegments(final long retentionMs) {
        deleteExpiredSegments(retentionMs, null);
    }

    public void deleteExpiredSegments(final long retentionMs, DeleteHook afterDeleted) {
        final long deleteUntil = System.currentTimeMillis() - retentionMs;
        Preconditions.checkState(deleteUntil > 0, "retentionMs不应该超过当前时间");

        Predicate<LogSegment> predicate = segment -> segment.getLastModifiedTime() < deleteUntil;
        deleteSegments(predicate, afterDeleted);
    }

    public void deleteSegmentsBeforeOffset(final long offset) {
        if (offset == -1) return;
        Predicate<LogSegment> predicate = segment -> segment.getBaseOffset() + segment.getFileSize() < offset;
        deleteSegments(predicate, null);
    }

    private void deleteSegmentsAfterOffset(final long offset) {
        Predicate<LogSegment> predicate = segment -> segment.getBaseOffset() > offset;
        deleteSegments(predicate, null);
    }

    public void deleteSegments(Predicate<LogSegment> predicate, DeleteHook afterDeleted) {
        int count = segments.size();
        if (count <= 1) return;

        for (final Map.Entry<Long, LogSegment> entry : segments.entrySet()) {
            if (count <= 1) return;

            final LogSegment segment = entry.getValue();

            if (predicate.test(segment)) {
                if (deleteSegment(entry.getKey(), segment)) {
                    count = count - 1;
                    executeHook(afterDeleted, segment);
                    LOG.info("remove expired segment success. segment: {}", segment);
                } else {
                    LOG.warn("remove expired segment failed. segment: {}", segment);
                    return;
                }
            }
        }
    }

    private void executeHook(DeleteHook hook, LogSegment segment) {
        if (hook == null) return;

        hook.afterDeleted(this, segment);
    }

    private boolean deleteSegment(final long key, final LogSegment segment) {
        if (!segment.disable()) return false;
        segments.remove(key);
        segment.destroy();
        return true;
    }

    public void destroy() {
        deleteAllSegments();
        logDir.delete();
    }

    public interface DeleteHook {
        void afterDeleted(final LogManager logManager, final LogSegment deletedSegment);
    }

    public boolean clean(Long key) {
        LogSegment segment = segments.get(key);
        if (null == segment) {
            LOG.error("clean message segment log error,segment:{} is null", key);
            return false;
        }

        if (deleteSegment(key, segment)) {
            LOG.info("remove expired segment success. segment: {}", segment);
            return true;
        } else {
            LOG.warn("remove expired segment failed. segment: {}", segment);
            return false;
        }
    }
}
