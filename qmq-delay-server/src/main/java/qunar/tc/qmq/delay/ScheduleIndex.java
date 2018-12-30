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

package qunar.tc.qmq.delay;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public class ScheduleIndex {

    private static final Interner<String> INTERNER = Interners.newStrongInterner();

    private final String subject;
    private final long scheduleTime;
    private final long offset;
    private final int size;
    private final long sequence;

    public ScheduleIndex(String subject, long scheduleTime, long offset, int size, long sequence) {
        this.subject = INTERNER.intern(subject);
        this.scheduleTime = scheduleTime;
        this.offset = offset;
        this.size = size;
        this.sequence = sequence;
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public long getSequence() {
        return sequence;
    }

    public String getSubject() {
        return subject;
    }
}
