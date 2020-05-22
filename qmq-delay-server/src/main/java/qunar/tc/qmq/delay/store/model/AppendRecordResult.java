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

import qunar.tc.qmq.store.AppendMessageStatus;

import java.nio.ByteBuffer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 12:01
 */
public class AppendRecordResult<T> {
    private final AppendMessageStatus status;
    private final long wroteOffset;
    private final int wroteBytes;
    private final ByteBuffer buffer;
    private T additional;

    public AppendRecordResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, ByteBuffer buffer) {
        this(status, wroteOffset, wroteBytes, buffer, null);
    }

    public AppendRecordResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, ByteBuffer buffer, T additional) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.buffer = buffer;
        this.additional = additional;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public T getAdditional() {
        return additional;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

}
