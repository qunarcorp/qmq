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

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public class SegmentBuffer {
    private final long startOffset;
    private final int size;

    private final ByteBuffer buffer;
    private final LogSegment logSegment;

    private long wroteOffset;

    public SegmentBuffer(long startOffset, ByteBuffer buffer, int size, LogSegment logSegment) {
        this.startOffset = startOffset;
        this.size = size;
        this.buffer = buffer;
        this.logSegment = logSegment;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getSize() {
        return size;
    }

    public LogSegment getLogSegment() {
        return logSegment;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public boolean release() {
        return logSegment.release();
    }

    public boolean retain() {
        return logSegment.retain();
    }
}
