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

package qunar.tc.qmq.delay.wheel;

import java.util.Objects;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-09-28 11:21
 */
public class WheelLoadCursor {
    private volatile long baseOffset;
    private volatile long offset;

    private final Object cursorLock = new Object();

    public static WheelLoadCursor create() {
        return new WheelLoadCursor();
    }

    private WheelLoadCursor() {
        this.baseOffset = -1;
        this.offset = -1L;
    }

    boolean shiftCursor(long shiftBaseOffset, long shiftOffset) {
        if (shiftBaseOffset >= baseOffset) {
            synchronized (cursorLock) {
                this.baseOffset = shiftBaseOffset;
                this.offset = shiftOffset;
            }
            return true;
        }

        return false;
    }

    boolean shiftCursor(long cursor) {
        if (cursor >= baseOffset) {
            synchronized (cursorLock) {
                this.baseOffset = cursor;
                this.offset = -1;
            }
            return true;
        }

        return false;
    }

    boolean shiftOffset(long loadedOffset) {
        if (offset < loadedOffset) {
            synchronized (cursorLock) {
                offset = loadedOffset;
                return true;
            }
        }

        return false;
    }

    Cursor cursor() {
        synchronized (cursorLock) {
            return new Cursor(baseOffset, offset);
        }
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long offset() {
        synchronized (cursorLock) {
            return offset;
        }
    }

    public static class Cursor {
        private final long baseOffset;
        private final long offset;

        public Cursor(long baseOffset, long offset) {
            this.baseOffset = baseOffset;
            this.offset = offset;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public long getOffset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Cursor cursor = (Cursor) o;
            return baseOffset == cursor.baseOffset &&
                    offset == cursor.offset;
        }

        @Override
        public int hashCode() {

            return Objects.hash(baseOffset, offset);
        }
    }
}
