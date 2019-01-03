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

package qunar.tc.qmq.delay.store.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.cleaner.LogCleaner;
import qunar.tc.qmq.delay.config.StoreConfiguration;
import qunar.tc.qmq.delay.store.DelaySegmentValidator;
import qunar.tc.qmq.delay.store.appender.LogAppender;
import qunar.tc.qmq.delay.store.model.AppendDispatchRecordResult;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.RecordResult;
import qunar.tc.qmq.store.AppendMessageResult;
import qunar.tc.qmq.store.PutMessageStatus;
import qunar.tc.qmq.store.SegmentBuffer;
import qunar.tc.qmq.sync.DelaySyncRequest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static qunar.tc.qmq.delay.store.log.ScheduleOffsetResolver.resolveSegment;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 16:39
 */
public class DispatchLogSegmentContainer extends AbstractDelaySegmentContainer<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DispatchLogSegmentContainer.class);

    private final StoreConfiguration config;

    DispatchLogSegmentContainer(StoreConfiguration config, File logDir, DelaySegmentValidator validator, LogAppender<Boolean, LogRecord> appender) {
        super(config.getSegmentScale(), logDir, validator, appender);
        this.config = config;
    }

    @Override
    protected void loadLogs(DelaySegmentValidator validator) {
        LOGGER.info("Loading logs.");
        File[] files = this.logDir.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.getName().startsWith(".")) {
                    continue;
                }
                if (file.isDirectory()) {
                    continue;
                }

                DelaySegment<Boolean> segment;
                try {
                    segment = new DispatchLogSegment(file);
                    long size = validator.validate(segment);
                    segment.setWrotePosition(size);
                    segment.setFlushedPosition(size);
                    segments.put(segment.getSegmentBaseOffset(), segment);
                } catch (IOException e) {
                    LOGGER.error("Load {} failed.", file.getAbsolutePath(), e);
                }
            }
        }
        LOGGER.info("Load logs done.");
    }

    @Override
    protected RecordResult<Boolean> retResult(AppendMessageResult<Boolean> result) {
        switch (result.getStatus()) {
            case SUCCESS:
                return new AppendDispatchRecordResult(PutMessageStatus.SUCCESS, result);
            default:
                return new AppendDispatchRecordResult(PutMessageStatus.UNKNOWN_ERROR, result);
        }
    }

    @Override
    protected DelaySegment<Boolean> allocSegment(long segmentBaseOffset) {
        File nextSegmentFile = new File(logDir, String.valueOf(segmentBaseOffset));
        try {
            DelaySegment<Boolean> logSegment = new DispatchLogSegment(nextSegmentFile);
            segments.put(segmentBaseOffset, logSegment);
            LOGGER.info("alloc new dispatch log segment file {}", ((DispatchLogSegment) logSegment).fileName);
            return logSegment;
        } catch (IOException e) {
            LOGGER.error("Failed create new dispatch log segment file. file: {}", nextSegmentFile.getAbsolutePath(), e);
        }
        return null;
    }

    DispatchLogSegment latestSegment() {
        Map.Entry<Long, DelaySegment<Boolean>> entry = segments.lastEntry();
        if (null == entry) {
            return null;
        }

        return ((DispatchLogSegment) segments.lastEntry().getValue());
    }

    public void clean(LogCleaner.CleanHook hook) {
        long deleteUntil = resolveSegment(System.currentTimeMillis() - config.getDispatchLogKeepTime(), segmentScale);
        for (DelaySegment<Boolean> segment : segments.values()) {
            if (segment.getSegmentBaseOffset() < deleteUntil) {
                doClean(segment, hook);
            }
        }
    }

    private void doClean(DelaySegment<Boolean> segment, LogCleaner.CleanHook hook) {
        long segmentBaseOffset = segment.getSegmentBaseOffset();
        if (clean(segmentBaseOffset) && hook != null) {
            hook.clean(segmentBaseOffset);
        }
    }

    SegmentBuffer getDispatchData(long segmentBaseOffset, long dispatchLogOffset) {
        DispatchLogSegment segment = (DispatchLogSegment) segments.get(segmentBaseOffset);
        if (null == segment) {
            return null;
        }

        return segment.selectSegmentBuffer(dispatchLogOffset);
    }

    long getMaxOffset(long segmentOffset) {
        DispatchLogSegment segment = (DispatchLogSegment) segments.get(segmentOffset);
        if (null == segment) {
            return 0;
        }

        return segment.getWrotePosition();
    }

    DelaySyncRequest.DispatchLogSyncRequest getSyncMaxRequest() {
        final DispatchLogSegment segment = latestSegment();
        if (segment == null) {
            return null;
        }

        long lastBaseOffset = -1;
        long lastOffset = -1;
        final DispatchLogSegment lastSegment = lowerSegment(segment.getSegmentBaseOffset());
        if (lastSegment != null) {
            lastBaseOffset = lastSegment.getSegmentBaseOffset();
            lastOffset = lastSegment.getWrotePosition();
        }

        return new DelaySyncRequest.DispatchLogSyncRequest(segment.getSegmentBaseOffset(), segment.getWrotePosition(), lastBaseOffset, lastOffset);
    }

    boolean appendData(long startOffset, long baseOffset, ByteBuffer body) {
        DispatchLogSegment segment = (DispatchLogSegment) segments.get(baseOffset);
        if (null == segment) {
            segment = (DispatchLogSegment) allocSegment(baseOffset);
            segment.fillPreBlank(startOffset);
        }

        return segment.appendData(startOffset, body);
    }

    DispatchLogSegment lowerSegment(long offset) {
        Map.Entry<Long, DelaySegment<Boolean>> lowEntry = segments.lowerEntry(offset);
        if (lowEntry == null) {
            return null;
        }
        return (DispatchLogSegment) lowEntry.getValue();
    }
}
