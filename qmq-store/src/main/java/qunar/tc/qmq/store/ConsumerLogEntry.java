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

/**
 * Created by zhaohui.yu
 * 9/3/18
 */
public class ConsumerLogEntry {
    private long timestamp;
    private long wroteOffset;
    private int wroteBytes;
    private short headerSize;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public short getHeaderSize() {
        return headerSize;
    }

    public void setHeaderSize(short headerSize) {
        this.headerSize = headerSize;
    }

    public static class Factory {
        private static final ThreadLocal<ConsumerLogEntry> ENTRY = ThreadLocal.withInitial(() -> new ConsumerLogEntry());

        public static ConsumerLogEntry create() {
            return ENTRY.get();
        }
    }
}
