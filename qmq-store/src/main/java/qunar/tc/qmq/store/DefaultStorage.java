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

import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.action.ActionEvent;
import qunar.tc.qmq.store.action.MaxSequencesUpdater;
import qunar.tc.qmq.store.action.PullLogBuilder;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author keli.wang
 * @since 2017/7/4
 */
public class DefaultStorage implements Storage {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorage.class);

    private static final int DEFAULT_FLUSH_INTERVAL = 500; // ms

    private final StorageConfig config;

    private final MessageLog messageLog;
    private final ConsumerLogManager consumerLogManager;
    private final PullLogManager pullLogManager;
    private final ActionLog actionLog;
    private final ConsumeQueueManager consumeQueueManager;

    private final CheckpointManager checkpointManager;

    private final PullLogFlusher pullLogFlusher;
    private final FixedExecOrderEventBus actionEventBus;
    private final ActionLogIterateService actionLogIterateService;
    private final ConsumerLogFlusher consumerLogFlusher;
    private final FixedExecOrderEventBus messageEventBus;
    private final MessageLogIterateService messageLogIterateService;

    private final ScheduledExecutorService logCleanerExecutor;

    private final PeriodicFlushService messageLogFlushService;
    private final PeriodicFlushService actionLogFlushService;

    public DefaultStorage(final BrokerRole role, final StorageConfig config, final CheckpointLoader loader) {
        this.config = config;
        this.consumerLogManager = new ConsumerLogManager(config);
        this.messageLog = new MessageLog(config, consumerLogManager);
        this.pullLogManager = new PullLogManager(config);
        this.actionLog = new ActionLog(config);

        this.checkpointManager = new CheckpointManager(role, config, loader);
        this.checkpointManager.fixOldVersionCheckpointIfShould(consumerLogManager, pullLogManager);
        // must init after offset manager created
        this.consumeQueueManager = new ConsumeQueueManager(this);

        this.pullLogFlusher = new PullLogFlusher(config, checkpointManager, pullLogManager);
        this.actionEventBus = new FixedExecOrderEventBus();
        this.actionEventBus.subscribe(ActionEvent.class, new PullLogBuilder(this));
        this.actionEventBus.subscribe(ActionEvent.class, new MaxSequencesUpdater(checkpointManager));
        this.actionEventBus.subscribe(ActionEvent.class, pullLogFlusher);
        this.actionLogIterateService = new ActionLogIterateService(actionLog, checkpointManager, actionEventBus);

        this.consumerLogFlusher = new ConsumerLogFlusher(config, checkpointManager, consumerLogManager);
        this.messageEventBus = new FixedExecOrderEventBus();
        this.messageEventBus.subscribe(MessageLogMeta.class, new BuildConsumerLogEventListener(consumerLogManager));
        this.messageEventBus.subscribe(MessageLogMeta.class, consumerLogFlusher);
        this.messageLogIterateService = new MessageLogIterateService(messageLog, checkpointManager, messageEventBus);

        this.logCleanerExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("log-cleaner-%d").build());

        this.messageLogFlushService = new PeriodicFlushService(new MessageLogFlushProvider());
        this.actionLogFlushService = new PeriodicFlushService(new ActionLogFlushProvider());
    }

    @Override
    public void start() {
        messageLogFlushService.start();
        actionLogFlushService.start();
        actionLogIterateService.start();
        messageLogIterateService.start();

        messageLogIterateService.blockUntilReplayDone();
        actionLogIterateService.blockUntilReplayDone();
        // must call this after message log replay done
        consumerLogManager.initConsumerLogOffset();

        logCleanerExecutor.scheduleAtFixedRate(
                new LogCleaner(), 0, config.getLogRetentionCheckIntervalSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public StorageConfig getStorageConfig() {
        return config;
    }

    @Override
    public void destroy() {
        safeClose(actionLogIterateService);
        safeClose(messageLogIterateService);
        safeClose(messageLogFlushService);
        safeClose(actionLogFlushService);
        safeClose(consumerLogFlusher);
        safeClose(pullLogFlusher);
        safeClose(checkpointManager);
        safeClose(messageLog);
        safeClose(consumerLogManager);
        safeClose(pullLogManager);
    }

    private void safeClose(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignore) {
            LOG.debug("close resource failed");
        }
    }

    @Override
    public synchronized PutMessageResult appendMessage(RawMessage message) {
        return messageLog.putMessage(message);
    }

    @Override
    public SegmentBuffer getMessageData(long wroteOffset) {
        return messageLog.getMessageData(wroteOffset);
    }

    @Override
    public GetMessageResult getMessage(String subject, long sequence) {
        final MessageFilter always = entry -> true;
        return pollMessages(subject, sequence, 1, always, true);
    }

    @Override
    public GetMessageResult pollMessages(String subject, long startSequence, int maxMessages) {
        final MessageFilter always = entry -> true;
        return pollMessages(subject, startSequence, maxMessages, always);
    }

    @Override
    public GetMessageResult pollMessages(String subject, long consumerLogSequence, int maxMessages, MessageFilter filter) {
        return pollMessages(subject, consumerLogSequence, maxMessages, filter, false);
    }

    private GetMessageResult pollMessages(String subject, long consumerLogSequence, int maxMessages, MessageFilter filter, boolean strictly) {
        final GetMessageResult result = new GetMessageResult();

        if (maxMessages <= 0) {
            result.setNextBeginOffset(consumerLogSequence);
            result.setStatus(GetMessageStatus.NO_MESSAGE);
            return result;
        }

        final ConsumerLog consumerLog = consumerLogManager.getConsumerLog(subject);
        if (consumerLog == null) {
            result.setNextBeginOffset(0);
            result.setStatus(GetMessageStatus.SUBJECT_NOT_FOUND);
            return result;
        }

        final OffsetBound bound = consumerLog.getOffsetBound();
        final long minSequence = bound.getMinOffset();
        final long maxSequence = bound.getMaxOffset();
        result.setMinOffset(minSequence);
        result.setMaxOffset(maxSequence);

        if (maxSequence == 0) {
            result.setNextBeginOffset(maxSequence);
            result.setStatus(GetMessageStatus.NO_MESSAGE);
            return result;
        }

        if (consumerLogSequence < 0) {
            result.setConsumerLogRange(new OffsetRange(maxSequence, maxSequence));
            result.setNextBeginOffset(maxSequence);
            result.setStatus(GetMessageStatus.SUCCESS);
            return result;
        }

        if (consumerLogSequence > maxSequence) {
            result.setNextBeginOffset(maxSequence);
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW);
            return result;
        }

        if (strictly && consumerLogSequence < minSequence) {
            result.setNextBeginOffset(consumerLogSequence);
            result.setStatus(GetMessageStatus.EMPTY_CONSUMER_LOG);
            return result;
        }

        final long start = consumerLogSequence < minSequence ? minSequence : consumerLogSequence;
        final SegmentBuffer consumerLogBuffer = consumerLog.selectIndexBuffer(start);
        if (consumerLogBuffer == null) {
            result.setNextBeginOffset(start);
            result.setStatus(GetMessageStatus.EMPTY_CONSUMER_LOG);
            return result;
        }

        if (!consumerLogBuffer.retain()) {
            result.setNextBeginOffset(start);
            result.setStatus(GetMessageStatus.EMPTY_CONSUMER_LOG);
            return result;
        }

        long nextBeginSequence = start;
        try {
            final int maxMessagesInBytes = maxMessages * ConsumerLog.CONSUMER_LOG_UNIT_BYTES;
            for (int i = 0; i < maxMessagesInBytes; i += ConsumerLog.CONSUMER_LOG_UNIT_BYTES) {
                if (i >= consumerLogBuffer.getSize()) {
                    break;
                }

                final ConsumerLogEntry entry = ConsumerLogEntry.Factory.create();
                final ByteBuffer buffer = consumerLogBuffer.getBuffer();
                entry.setTimestamp(buffer.getLong());
                entry.setWroteOffset(buffer.getLong());
                entry.setWroteBytes(buffer.getInt());
                entry.setHeaderSize(buffer.getShort());

                if (!filter.filter(entry)) break;

                final SegmentBuffer messageBuffer = messageLog.getMessage(entry.getWroteOffset(), entry.getWroteBytes(), entry.getHeaderSize());
                if (messageBuffer != null && messageBuffer.retain()) {
                    result.addSegmentBuffer(messageBuffer);
                } else {
                    QMon.readMessageReturnNullCountInc(subject);
                    LOG.warn("read message log failed. consumerLogSequence: {}, wrote consumerLogSequence: {}, wrote bytes: {}, payload consumerLogSequence: {}",
                            nextBeginSequence, entry.getWroteOffset(), entry.getWroteBytes(), entry.getHeaderSize());

                    //如果前面已经获取到了消息，中间因为任何原因导致获取不到消息，则需要提前退出，避免consumer sequence中间出现空洞
                    if (result.getSegmentBuffers().size() > 0) {
                        break;
                    }
                }
                nextBeginSequence += 1;
            }
        } finally {
            consumerLogBuffer.release();
        }
        result.setNextBeginOffset(nextBeginSequence);
        result.setConsumerLogRange(new OffsetRange(start, nextBeginSequence - 1));
        result.setStatus(GetMessageStatus.SUCCESS);
        return result;
    }

    @Override
    public long getMaxMessageOffset() {
        return messageLog.getMaxOffset();
    }

    @Override
    public long getMinMessageOffset() {
        return messageLog.getMinOffset();
    }

    @Override
    public long getMaxActionLogOffset() {
        return actionLog.getMaxOffset();
    }

    public long getMinActionLogOffset() {
        return actionLog.getMinOffset();
    }

    @Override
    public long getMaxMessageSequence(String subject) {
        final ConsumerLog consumerLog = consumerLogManager.getConsumerLog(subject);
        if (consumerLog == null) {
            return 0;
        } else {
            return consumerLog.nextSequence();
        }
    }

    @Override
    public PutMessageResult putAction(Action action) {
        return actionLog.addAction(action);
    }

    @Override
    public List<PutMessageResult> putPullLogs(String subject, String group, String consumerId, List<PullLogMessage> messages) {
        final PullLog pullLog = pullLogManager.getOrCreate(subject, group, consumerId);
        return pullLog.putPullLogMessages(messages);
    }

    @Override
    public CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    @Override
    public ConsumerGroupProgress getConsumerGroupProgress(final String subject, final String group) {
        return checkpointManager.getConsumerGroupProgress(subject, group);
    }

    @Override
    public Collection<ConsumerGroupProgress> allConsumerGroupProgresses() {
        return checkpointManager.allConsumerGroupProgresses();
    }

    @Override
    public long getMaxPulledMessageSequence(String subject, String group) {
        return checkpointManager.getMaxPulledMessageSequence(subject, group);
    }

    @Override
    public long getMessageSequenceByPullLog(String subject, String group, String consumerId, long pullLogSequence) {
        final PullLog log = pullLogManager.get(subject, group, consumerId);
        if (log == null) {
            return -1;
        }

        return log.getMessageSequence(pullLogSequence);
    }

    @Override
    public void updateConsumeQueue(String subject, String group, int consumeFromWhereCode) {
        final ConsumerLog consumerLog = consumerLogManager.getConsumerLog(subject);
        if (consumerLog == null) {
            LOG.warn("没有对应的consumerLog, subject:{}", subject);
            return;
        }
        final ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.codeOf(consumeFromWhereCode);
        final OffsetBound bound = consumerLog.getOffsetBound();
        switch (consumeFromWhere) {
            case UNKNOWN:
                LOG.info("UNKNOWN consumeFromWhere code, {}", consumeFromWhereCode);
                break;
            case EARLIEST:
                consumeQueueManager.update(subject, group, bound.getMinOffset());
                break;
            case LATEST:
                consumeQueueManager.update(subject, group, bound.getMaxOffset());
                break;
        }
    }

    @Override
    public void disableLagMonitor(String subject, String group) {
        consumeQueueManager.disableLagMonitor(subject, group);
    }

    @Override
    public Table<String, String, PullLog> allPullLogs() {
        return pullLogManager.getLogs();
    }

    @Override
    public void destroyPullLog(String subject, String group, String consumerId) {
        if (pullLogManager.destroy(subject, group, consumerId)) {
            checkpointManager.removeConsumerProgress(subject, group, consumerId);
        }
    }

    @Override
    public ConsumeQueue locateConsumeQueue(String subject, String group) {
        return consumeQueueManager.getOrCreate(subject, group);
    }

    @Override
    public Map<String, ConsumeQueue> locateSubjectConsumeQueues(String subject) {
        return consumeQueueManager.getBySubject(subject);
    }

    @Override
    public <T> void registerEventListener(final Class<T> clazz, final FixedExecOrderEventBus.Listener<T> listener) {
        messageEventBus.subscribe(clazz, listener);
    }

    @Override
    public void registerActionEventListener(final FixedExecOrderEventBus.Listener<ActionEvent> listener) {
        actionEventBus.subscribe(ActionEvent.class, listener);
    }

    @Override
    public SegmentBuffer getActionLogData(long offset) {
        return actionLog.getMessageData(offset);
    }

    @Override
    public boolean appendMessageLogData(long startOffset, ByteBuffer data) {
        return messageLog.appendData(startOffset, data);
    }

    @Override
    public boolean appendActionLogData(long startOffset, ByteBuffer data) {
        return actionLog.appendData(startOffset, data);
    }

    private class BuildConsumerLogEventListener implements FixedExecOrderEventBus.Listener<MessageLogMeta> {
        private final ConsumerLogManager consumerLogManager;
        private final Map<String, Long> offsets;

        private BuildConsumerLogEventListener(final ConsumerLogManager consumerLogManager) {
            this.consumerLogManager = consumerLogManager;
            // TODO(keli.wang): is load offset from consumer log enough?
            this.offsets = new HashMap<>(consumerLogManager.currentConsumerLogOffset());
        }

        @Override
        public void onEvent(final MessageLogMeta event) {
            if (isFirstEventOfLogSegment(event)) {
                LOG.info("first event of log segment. event: {}", event);
                // TODO(keli.wang): need catch all exception here?
                consumerLogManager.createOffsetFileFor(event.getBaseOffset(), offsets);
            }

            updateOffset(event);

            final ConsumerLog consumerLog = consumerLogManager.getOrCreateConsumerLog(event.getSubject());
            if (consumerLog.nextSequence() != event.getSequence()) {
                LOG.error("next sequence not equals to max sequence. subject: {}, received seq: {}, received offset: {}, diff: {}",
                        event.getSubject(), event.getSequence(), event.getWroteOffset(), event.getSequence() - consumerLog.nextSequence());
            }
            final boolean success = consumerLog.putMessageLogOffset(event.getSequence(), event.getWroteOffset(), event.getWroteBytes(), event.getHeaderSize());
            checkpointManager.updateMessageReplayState(event);
            messageEventBus.post(new ConsumerLogWroteEvent(event.getSubject(), success));
        }

        private boolean isFirstEventOfLogSegment(final MessageLogMeta event) {
            return event.getWroteOffset() == event.getBaseOffset();
        }

        private void updateOffset(final MessageLogMeta meta) {
            final String subject = meta.getSubject();
            final long sequence = meta.getSequence();
            if (offsets.containsKey(subject)) {
                offsets.merge(subject, sequence, Math::max);
            } else {
                offsets.put(subject, sequence);
            }
        }
    }

    private class MessageLogFlushProvider implements PeriodicFlushService.FlushProvider {
        @Override
        public int getInterval() {
            return DEFAULT_FLUSH_INTERVAL;
        }

        @Override
        public void flush() {
            messageLog.flush();
        }
    }

    private class LogCleaner implements Runnable {

        @Override
        public void run() {
            try {
                messageLog.clean();
                consumerLogManager.clean();
                pullLogManager.clean(allConsumerGroupProgresses());
                actionLog.clean();
            } catch (Throwable e) {
                LOG.error("log cleaner caught exception.", e);
            }
        }
    }

    private class ActionLogFlushProvider implements PeriodicFlushService.FlushProvider {
        @Override
        public int getInterval() {
            return DEFAULT_FLUSH_INTERVAL;
        }

        @Override
        public void flush() {
            actionLog.flush();
        }
    }
}
