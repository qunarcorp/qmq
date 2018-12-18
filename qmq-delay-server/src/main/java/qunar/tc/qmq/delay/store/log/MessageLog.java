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
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.delay.config.StoreConfiguration;
import qunar.tc.qmq.delay.monitor.QMon;
import qunar.tc.qmq.delay.store.VisitorAccessor;
import qunar.tc.qmq.delay.store.model.AppendLogResult;
import qunar.tc.qmq.delay.store.model.AppendMessageRecordResult;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.RawMessageExtend;
import qunar.tc.qmq.delay.store.visitor.LogVisitor;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.store.PeriodicFlushService;
import qunar.tc.qmq.store.PutMessageStatus;
import qunar.tc.qmq.store.SegmentBuffer;

import java.nio.ByteBuffer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 9:29
 */
public class MessageLog implements Log<MessageLog.MessageRecordMeta, RawMessageExtend>, VisitorAccessor<Long> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageLog.class);

    private static final int DEFAULT_FLUSH_INTERVAL = 500;

    private final SegmentContainer<AppendMessageRecordResult, RawMessageExtend> container;

    public MessageLog(StoreConfiguration config) {
        this.container = new MessageSegmentContainer(config);
    }

    @Override
    public AppendLogResult<MessageLog.MessageRecordMeta> append(RawMessageExtend record) {
        long start = System.currentTimeMillis();
        MessageHeader header = record.getHeader();
        String messageId = header.getMessageId();
        String subject = header.getSubject();

        AppendMessageRecordResult recordResult;
        try {
            recordResult = container.append(record);
            PutMessageStatus status = recordResult.getStatus();
            if (PutMessageStatus.MESSAGE_ILLEGAL == status) {
                LOGGER.error("appendMessageLog message log error,log:{} {}", subject, messageId);
                appendFailedByMessageIllegal(subject);
                return new AppendLogResult<>(MessageProducerCode.SUCCESS, status.name(), new MessageLog.MessageRecordMeta(messageId, -1));
            }

            if (PutMessageStatus.SUCCESS != status) {
                LOGGER.error("appendMessageLog message log error,log:{} {},status:{}", subject, messageId, status.name());
                appendFailed(subject);
                return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, status.name(), new MessageLog.MessageRecordMeta(messageId, -1));
            }

            return new AppendLogResult<>(MessageProducerCode.SUCCESS, status.name(), new MessageLog.MessageRecordMeta(messageId, recordResult.getResult().getAdditional()));
        } catch (Throwable e) {
            LOGGER.error("appendMessageLog message log error,log:{} {}, msg:{}", subject, messageId, record, e);
            appendFailed(subject);
            return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, "", new MessageLog.MessageRecordMeta(messageId, -1));
        } finally {
            appendTimer(subject, System.currentTimeMillis() - start);
        }
    }

    private void appendFailedByMessageIllegal(String subject) {
        QMon.appendFailedByMessageIllegal(subject);
    }

    @Override
    public boolean clean(Long key) {
        return container.clean(key);
    }

    @Override
    public void flush() {
        container.flush();
    }

    @Override
    public LogVisitor<LogRecord> newVisitor(Long key) {
        return ((MessageSegmentContainer) container).newLogVisitor(key);
    }

    @Override
    public long getMaxOffset() {
        return ((MessageSegmentContainer) container).getMaxOffset();
    }

    public PeriodicFlushService.FlushProvider getProvider() {
        return new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return DEFAULT_FLUSH_INTERVAL;
            }

            @Override
            public void flush() {
                MessageLog.this.flush();
            }
        };
    }

    public void clean() {
        ((MessageSegmentContainer) container).clean();
    }

    private static void appendFailed(String subject) {
        QMon.appendFailed(subject);
    }

    private static void appendTimer(String subject, long cost) {
        QMon.appendTimer(subject, cost);
    }

    public long getMinOffset() {
        return ((MessageSegmentContainer) container).getMinOffset();
    }

    public SegmentBuffer getMessageLogData(long startSyncOffset) {
        return ((MessageSegmentContainer) container).getMessageData(startSyncOffset);
    }

    public boolean appendData(long startOffset, ByteBuffer buffer) {
        return ((MessageSegmentContainer) container).appendData(startOffset, buffer);
    }

    public static class MessageRecordMeta {
        private String messageId;

        private long messageOffset;

        MessageRecordMeta(String messageId, long messageOffset) {
            this.messageId = messageId;
            this.messageOffset = messageOffset;
        }

        public String getMessageId() {
            return messageId;
        }

        public long getMessageOffset() {
            return messageOffset;
        }
    }
}
