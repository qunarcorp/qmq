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

package qunar.tc.qmq.delay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.base.AppendException;
import qunar.tc.qmq.delay.base.LongHashSet;
import qunar.tc.qmq.delay.base.ReceivedDelayMessage;
import qunar.tc.qmq.delay.base.ReceivedResult;
import qunar.tc.qmq.delay.cleaner.LogCleaner;
import qunar.tc.qmq.delay.config.StoreConfiguration;
import qunar.tc.qmq.delay.store.IterateOffsetManager;
import qunar.tc.qmq.delay.store.log.*;
import qunar.tc.qmq.delay.store.model.AppendLogResult;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.RawMessageExtend;
import qunar.tc.qmq.delay.store.model.ScheduleSetRecord;
import qunar.tc.qmq.delay.wheel.WheelLoadCursor;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.store.LogIterateService;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.sync.DelaySyncRequest;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-20 10:20
 */
public class DefaultDelayLogFacade implements DelayLogFacade {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDelayLogFacade.class);
    private final IterateOffsetManager offsetManager;
    private final ScheduleLog scheduleLog;
    private final DispatchLog dispatchLog;
    private final MessageLog messageLog;
    private final LogFlusher logFlusher;
    private final LogCleaner cleaner;
    private final LogIterateService<LogRecord> messageLogIterateService;

    public DefaultDelayLogFacade(final StoreConfiguration config, final Function<ScheduleIndex, Boolean> func) {
        this.messageLog = new MessageLog(config);
        this.scheduleLog = new ScheduleLog(config);
        this.dispatchLog = new DispatchLog(config);
        this.offsetManager = new IterateOffsetManager(config.getCheckpointStorePath(), scheduleLog::flush);
        FixedExecOrderEventBus bus = new FixedExecOrderEventBus();
        bus.subscribe(LogRecord.class, e -> {
            AppendLogResult<ScheduleIndex> result = appendScheduleLog(e);
            int code = result.getCode();
            if (MessageProducerCode.SUCCESS != code) {
                LOGGER.error("appendMessageLog schedule log error,log:{} {},code:{}", e.getSubject(), e.getMessageId(), code);
                throw new AppendException("appendScheduleLogError");
            }
            func.apply(result.getAdditional());
        });
        bus.subscribe(LogRecord.class, e -> {
            long checkpoint = e.getStartWroteOffset() + e.getRecordSize();
            updateIterateOffset(checkpoint);
        });
        this.messageLogIterateService = new LogIterateService<>("message-log", 5, messageLog, initialMessageIterateFrom(), bus);
        this.logFlusher = new LogFlusher(messageLog, offsetManager, dispatchLog);
        this.cleaner = new LogCleaner(config, dispatchLog, scheduleLog, messageLog);
    }

    @Override
    public void start() {
        logFlusher.start();
        messageLogIterateService.start();
        cleaner.start();
    }

    @Override
    public ReceivedResult appendMessageLog(final ReceivedDelayMessage message) {
        final RawMessageExtend rawMessage = message.getMessage();
        final String msgId = rawMessage.getHeader().getMessageId();
        AppendLogResult<MessageLog.MessageRecordMeta> result = messageLog.append(rawMessage);
        return new ReceivedResult(msgId, result.getCode(), result.getRemark(), result.getAdditional().getMessageOffset());
    }

    @Override
    public long getMessageLogMinOffset() {
        return messageLog.getMinOffset();
    }

    @Override
    public long getMessageLogMaxOffset() {
        return messageLog.getMaxOffset();
    }

    @Override
    public long getDispatchLogMaxOffset(final long dispatchSegmentBaseOffset) {
        return dispatchLog.getMaxOffset(dispatchSegmentBaseOffset);
    }

    @Override
    public DelaySyncRequest.DispatchLogSyncRequest getDispatchLogSyncMaxRequest() {
        return dispatchLog.getSyncMaxRequest();
    }

    @Override
    public boolean appendMessageLogData(final long startOffset, final ByteBuffer buffer) {
        return messageLog.appendData(startOffset, buffer);
    }

    @Override
    public boolean appendDispatchLogData(final long startOffset, final long baseOffset, final ByteBuffer body) {
        return dispatchLog.appendData(startOffset, baseOffset, body);
    }

    @Override
    public SegmentBuffer getMessageLogs(final long startSyncOffset) {
        return messageLog.getMessageLogData(startSyncOffset);
    }

    @Override
    public SegmentBuffer getDispatchLogs(final long segmentBaseOffset, final long dispatchLogOffset) {
        return dispatchLog.getDispatchLogData(segmentBaseOffset, dispatchLogOffset);
    }


    @Override
    public void shutdown() {
        cleaner.shutdown();
        messageLogIterateService.close();
        logFlusher.shutdown();
        scheduleLog.destroy();
    }

    @Override
    public List<ScheduleSetRecord> recoverLogRecord(final List<ScheduleIndex> indexList) {
        return scheduleLog.recoverLogRecord(indexList);
    }

    @Override
    public void appendDispatchLog(LogRecord record) {
        dispatchLog.append(record);
    }

    @Override
    public DispatchLogSegment latestDispatchSegment() {
        return dispatchLog.latestSegment();
    }

    @Override
    public DispatchLogSegment lowerDispatchSegment(final long baseOffset) {
        return dispatchLog.lowerSegment(baseOffset);
    }

    @Override
    public ScheduleSetSegment loadScheduleLogSegment(final long segmentBaseOffset) {
        return scheduleLog.loadSegment(segmentBaseOffset);
    }

    @Override
    public WheelLoadCursor.Cursor loadUnDispatch(final ScheduleSetSegment setSegment, final LongHashSet dispatchedSet, final Consumer<ScheduleIndex> refresh) {
        return scheduleLog.loadUnDispatch(setSegment, dispatchedSet, refresh);
    }

    @Override
    public long higherScheduleBaseOffset(long index) {
        return scheduleLog.higherBaseOffset(index);
    }

    @Override
    public long higherDispatchLogBaseOffset(long segmentBaseOffset) {
        return dispatchLog.higherBaseOffset(segmentBaseOffset);
    }

    @Override
    public AppendLogResult<ScheduleIndex> appendScheduleLog(LogRecord event) {
        return scheduleLog.append(event);
    }

    @Override
    public long initialMessageIterateFrom() {
        long iterateOffset = offsetManager.getIterateOffset();
        if (iterateOffset <= 0) {
            return getMessageLogMaxOffset();
        }
        if (iterateOffset > getMessageLogMaxOffset()) {
            return getMessageLogMaxOffset();
        }
        return iterateOffset;
    }

    @Override
    public void updateIterateOffset(long checkpoint) {
        offsetManager.updateIterateOffset(checkpoint);
    }

    @Override
    public void blockUntilReplayDone() {
        messageLogIterateService.blockUntilReplayDone();
    }
}
