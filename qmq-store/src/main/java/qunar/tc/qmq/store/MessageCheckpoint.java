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

import java.util.Map;

/**
 * @author keli.wang
 * @since 2018/9/12
 */
public class MessageCheckpoint {
    private final Map<String, Long> maxSequences;
    // mark this object is read from old version snapshot file
    // TODO(keli.wang): delete this after all broker group is migrate to new snapshot format
    private boolean fromOldVersion;
    private long offset;

    public MessageCheckpoint(long offset, Map<String, Long> maxSequences) {
        this(false, offset, maxSequences);
    }

    public MessageCheckpoint(boolean fromOldVersion, long offset, Map<String, Long> maxSequences) {
        this.fromOldVersion = fromOldVersion;
        this.offset = offset;
        this.maxSequences = maxSequences;
    }

    public boolean isFromOldVersion() {
        return fromOldVersion;
    }

    public void setFromOldVersion(boolean fromOldVersion) {
        this.fromOldVersion = fromOldVersion;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Map<String, Long> getMaxSequences() {
        return maxSequences;
    }
}
