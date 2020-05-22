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

package qunar.tc.qmq.delay.store.appender;

import qunar.tc.qmq.delay.store.model.AppendRecordResult;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.ScheduleSetSequence;
import qunar.tc.qmq.store.AppendMessageStatus;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 15:21
 */
public class ScheduleSetAppender implements LogAppender<ScheduleSetSequence, LogRecord> {

    private final ByteBuffer workingBuffer;
    private final ReentrantLock lock = new ReentrantLock();

    public ScheduleSetAppender(int singleMessageSize) {
        this.workingBuffer = ByteBuffer.allocate(singleMessageSize);
    }

    @Override
    public AppendRecordResult<ScheduleSetSequence> appendLog(LogRecord log) {
        workingBuffer.clear();
        workingBuffer.flip();
        final byte[] subjectBytes = log.getSubject().getBytes(StandardCharsets.UTF_8);
        final byte[] messageIdBytes = log.getMessageId().getBytes(StandardCharsets.UTF_8);
        int recordSize = getRecordSize(log, subjectBytes.length, messageIdBytes.length);
        workingBuffer.limit(recordSize);

        long scheduleTime = log.getScheduleTime();
        long sequence = log.getSequence();
        workingBuffer.putLong(scheduleTime);
        workingBuffer.putLong(sequence);
        workingBuffer.putInt(log.getPayloadSize());
        workingBuffer.putInt(messageIdBytes.length);
        workingBuffer.put(messageIdBytes);
        workingBuffer.putInt(subjectBytes.length);
        workingBuffer.put(subjectBytes);
        workingBuffer.put(log.getRecord());
        workingBuffer.flip();
        ScheduleSetSequence record = new ScheduleSetSequence(scheduleTime, sequence);
        return new AppendRecordResult<>(AppendMessageStatus.SUCCESS, 0, recordSize, workingBuffer, record);
    }

    private int getRecordSize(LogRecord record, int subject, int messageId) {
        return 8 + 8
                + 4
                + 4
                + 4
                + subject
                + messageId
                + record.getPayloadSize();
    }

    @Override
    public void lockAppender() {
        lock.lock();
    }

    @Override
    public void unlockAppender() {
        lock.unlock();
    }
}
