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

package qunar.tc.qmq.consumer.pull;

/**
 * @author yiqun.fan create on 17-8-25.
 */
class AckSendEntry {
    private final long pullOffsetBegin;
    private final long pullOffsetLast;
    private final boolean isBroadcast;

    public AckSendEntry() {
        this.pullOffsetBegin = -1;
        this.pullOffsetLast = -1;
        this.isBroadcast = false;
    }

    public AckSendEntry(AckEntry first, AckEntry last, boolean isBroadcast) {
        this(first.pullOffset(), last.pullOffset(), isBroadcast);
    }

    public AckSendEntry(long pullOffsetBegin, long pullOffsetLast, boolean isBroadcast) {
        this.pullOffsetBegin = pullOffsetBegin;
        this.pullOffsetLast = pullOffsetLast;
        this.isBroadcast = isBroadcast;
    }

    public long getPullOffsetBegin() {
        return pullOffsetBegin;
    }

    public long getPullOffsetLast() {
        return pullOffsetLast;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    @Override
    public String toString() {
        return "[" + pullOffsetBegin + ", " + pullOffsetLast + "]";
    }
}
