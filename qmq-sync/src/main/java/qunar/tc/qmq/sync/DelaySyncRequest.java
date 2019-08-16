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

package qunar.tc.qmq.sync;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-10 11:44
 */
public class DelaySyncRequest {
    private final long messageLogOffset;
    private final DispatchLogSyncRequest dispatchSyncRequest;
    private final int syncType;

    public DelaySyncRequest(long messageLogOffset, DispatchLogSyncRequest dispatchLogSyncRequest, int syncType) {
        this.messageLogOffset = messageLogOffset;
        this.dispatchSyncRequest = dispatchLogSyncRequest;
        this.syncType = syncType;
    }

    public DelaySyncRequest(long messageLogOffset, long dispatchLogSegmentBaseOffset, long dispatchLogOffset, long lastDispatchLogBaseOffset, long lastDispatchLogOffset, int syncType) {
        this.messageLogOffset = messageLogOffset;
        this.dispatchSyncRequest = new DispatchLogSyncRequest(dispatchLogSegmentBaseOffset, dispatchLogOffset, lastDispatchLogBaseOffset, lastDispatchLogOffset);
        this.syncType = syncType;
    }

    public long getMessageLogOffset() {
        return messageLogOffset;
    }

    public long getDispatchSegmentBaseOffset() {
        return dispatchSyncRequest == null ? -1 : dispatchSyncRequest.getSegmentBaseOffset();
    }

    public long getDispatchLogOffset() {
        return dispatchSyncRequest == null ? -1 : dispatchSyncRequest.getDispatchLogOffset();
    }

    public long getLastDispatchSegmentBaseOffset() {
        return dispatchSyncRequest == null ? -1 : dispatchSyncRequest.getLastSegmentBaseOffset();
    }

    public long getLastDispatchSegmentOffset() {
        return dispatchSyncRequest == null ? -1 : dispatchSyncRequest.getLastDispatchLogOffset();
    }

    public int getSyncType() {
        return syncType;
    }

    @Override
    public String toString() {
        return "DelaySyncRequest{" +
                "messageLogOffset=" + messageLogOffset +
                ", dispatchSyncRequest=" + dispatchSyncRequest +
                ", syncType=" + syncType +
                '}';
    }

    public static class DispatchLogSyncRequest {
        private final long segmentBaseOffset;
        private final long dispatchLogOffset;
        private final long lastSegmentBaseOffset;
        private final long lastDispatchLogOffset;

        public DispatchLogSyncRequest(long segmentBaseOffset, long dispatchLogOffset, long lastSegmentBaseOffset, long lastDispatchLogOffset) {
            this.segmentBaseOffset = segmentBaseOffset;
            this.dispatchLogOffset = dispatchLogOffset;
            this.lastSegmentBaseOffset = lastSegmentBaseOffset;
            this.lastDispatchLogOffset = lastDispatchLogOffset;
        }

        public long getSegmentBaseOffset() {
            return segmentBaseOffset;
        }

        public long getDispatchLogOffset() {
            return dispatchLogOffset;
        }

        public long getLastSegmentBaseOffset() {
            return lastSegmentBaseOffset;
        }

        public long getLastDispatchLogOffset() {
            return lastDispatchLogOffset;
        }

        @Override
        public String toString() {
            return "DispatchLogSyncRequest{" +
                    "segmentBaseOffset=" + segmentBaseOffset +
                    ", dispatchLogOffset=" + dispatchLogOffset +
                    ", lastSegmentBaseOffset=" + lastSegmentBaseOffset +
                    ", lastDispatchLogOffset=" + lastDispatchLogOffset +
                    '}';
        }
    }
}