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

package qunar.tc.qmq.delay.base;

import qunar.tc.qmq.store.LogSegment;
import qunar.tc.qmq.store.SegmentBuffer;

import java.nio.ByteBuffer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-13 14:32
 */
public class SegmentBufferExtend extends SegmentBuffer {
    private long baseOffset;

    public SegmentBufferExtend(long startOffset, ByteBuffer buffer, int size, long baseOffset, LogSegment logSegment) {
        super(startOffset, buffer, size, logSegment);
        this.baseOffset = baseOffset;
    }

    public long getBaseOffset() {
        return baseOffset;
    }
}
