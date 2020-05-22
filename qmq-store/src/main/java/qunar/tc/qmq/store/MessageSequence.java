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
 * @author keli.wang
 * @since 2017/8/19
 */
public class MessageSequence {
    private final long sequence;
    private final long physicalOffset;

    public MessageSequence(long sequence, long physicalOffset) {
        this.sequence = sequence;
        this.physicalOffset = physicalOffset;
    }

    public long getSequence() {
        return sequence;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }
}
