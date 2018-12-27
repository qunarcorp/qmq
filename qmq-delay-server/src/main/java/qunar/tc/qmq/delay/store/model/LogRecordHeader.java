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

package qunar.tc.qmq.delay.store.model;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-14 11:14
 */
public class LogRecordHeader {
    private final String subject;
    private final String messageId;
    private final long scheduleTime;
    private final long sequence;

    public LogRecordHeader(String subject, String messageId, long scheduleTime, long sequence) {
        this.subject = subject;
        this.messageId = messageId;
        this.scheduleTime = scheduleTime;
        this.sequence = sequence;
    }

    public String getSubject() {
        return subject;
    }

    public String getMessageId() {
        return messageId;
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "LogRecordHeader{" +
                "subject=" + subject +
                "messageId=" + messageId +
                ", scheduleTime=" + scheduleTime +
                ", sequence=" + sequence +
                '}';
    }
}
