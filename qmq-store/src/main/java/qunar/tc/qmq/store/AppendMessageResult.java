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
 * @since 2017/7/4
 */
public class AppendMessageResult<T> {
    private final AppendMessageStatus status;
    private final long wroteOffset;
    private final int wroteBytes;
    private final T additional;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, null);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset) {
        this(status, wroteOffset, 0, null);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes) {
        this(status, wroteOffset, wroteBytes, null);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, T additional) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.additional = additional;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public T getAdditional() {
        return additional;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", wroteOffset=" + wroteOffset +
                ", wroteBytes=" + wroteBytes +
                ", additional=" + additional +
                '}';
    }
}
