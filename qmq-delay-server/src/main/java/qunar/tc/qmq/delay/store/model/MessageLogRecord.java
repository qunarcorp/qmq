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


import java.nio.ByteBuffer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-12 10:20
 */
public class MessageLogRecord implements LogRecord {
    private LogRecordHeader header;
    private int wroteBytes;
    private long startWroteOffset;
    private int payloadSize;
    private ByteBuffer buffer;

    public MessageLogRecord(LogRecordHeader header, int wroteBytes, long startWroteOffset, int payloadSize, ByteBuffer buffer) {
        this.header = header;
        this.wroteBytes = wroteBytes;
        this.startWroteOffset = startWroteOffset;
        this.payloadSize = payloadSize;
        this.buffer = buffer;
    }

    @Override
    public long getStartWroteOffset() {
        return startWroteOffset;
    }

    @Override
    public int getRecordSize() {
        return wroteBytes;
    }

    @Override
    public int getPayloadSize() {
        return payloadSize;
    }

    @Override
    public ByteBuffer getRecord() {
        return buffer;
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
    public long getSequence() {
        return header.getSequence();
    }

}
