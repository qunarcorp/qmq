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
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-28 16:23
 */
class IndexCheckpoint {
    private long msgOffset;
    private long indexOffset;

    IndexCheckpoint(long msgOffset, long indexOffset) {
        this.msgOffset = msgOffset;
        this.indexOffset = indexOffset;
    }

    long getMsgOffset() {
        return msgOffset;
    }

    void setMsgOffset(long msgOffset) {
        this.msgOffset = msgOffset;
    }

    long getIndexOffset() {
        return indexOffset;
    }

    void setIndexOffset(long indexOffset) {
        this.indexOffset = indexOffset;
    }
}
