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

import qunar.tc.qmq.store.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public class GetMessageResult {
    private final List<Buffer> buffers = new ArrayList<>(100);
    private int bufferTotalSize = 0;

    private GetMessageStatus status;
    private long nextBeginSequence;

    private OffsetRange consumerLogRange;

    public GetMessageResult() {
    }

    public GetMessageResult(GetMessageStatus status) {
        this.status = status;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public List<Buffer> getBuffers() {
        return buffers;
    }

    public void addBuffer(final Buffer buffer) {
        buffers.add(buffer);
        bufferTotalSize += buffer.getSize();
    }

    public int getMessageNum() {
        return buffers.size();
    }

    public long getNextBeginSequence() {
        return nextBeginSequence;
    }

    public void setNextBeginSequence(long nextBeginSequence) {
        this.nextBeginSequence = nextBeginSequence;
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public OffsetRange getConsumerLogRange() {
        return consumerLogRange;
    }

    public void setConsumerLogRange(OffsetRange consumerLogRange) {
        this.consumerLogRange = consumerLogRange;
    }

    public void release() {
        for (Buffer buffer : buffers) {
            buffer.release();
        }
    }

    @Override
    public String toString() {
        return "GetMessageResult{" +
                "buffers=" + buffers.size() +
                ", bufferTotalSize=" + bufferTotalSize +
                ", status=" + status +
                ", nextBeginSequence=" + nextBeginSequence +
                ", consumerLogRange=" + consumerLogRange +
                '}';
    }
}
