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


import qunar.tc.qmq.delay.store.log.DirectBufCloser;

import java.nio.ByteBuffer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-13 11:00
 */
public class ScheduleSetRecord implements LogRecord {
    private final LogRecordHeader header;
    private final long startOffset;
    private final int recordSize;

    private final ByteBuffer record;

    public ScheduleSetRecord(String messageId, String subject, long scheduleTime, long startOffset, int recordSize, long sequence, ByteBuffer record) {
        this.header = new LogRecordHeader(subject, messageId, scheduleTime, sequence);
        this.startOffset = startOffset;
        this.recordSize = recordSize;
        this.record = record;
    }

    @Override
    public String getSubject() {
        return header.getSubject();
    }

    @Override
    public String getMessageId() {
        return header.getMessageId();
    }

    @Override
    public long getScheduleTime() {
        return header.getScheduleTime();
    }

    @Override
    public int getPayloadSize() {
        return -1;
    }

    @Override
    public ByteBuffer getRecord() {
        return record;
    }

    @Override
    public long getStartWroteOffset() {
        return startOffset;
    }

    @Override
    public int getRecordSize() {
        return recordSize;
    }

    @Override
    public long getSequence() {
        return header.getSequence();
    }

    public void release() {
        DirectBufCloser.close(record);
    }

    @Override
    public String toString() {
        return "ScheduleSetRecord{" +
                "header=" + header +
                ", recordSize=" + recordSize +
                ", record=" + record +
                '}';
    }
}
