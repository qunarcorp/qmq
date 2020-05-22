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

package qunar.tc.qmq.base;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public class SyncRequest {
    private final long messageLogOffset;
    private final long actionLogOffset;
    private final int syncType;

    public SyncRequest(int syncType, long messageLogOffset, long actionLogOffset) {
        this.syncType = syncType;
        this.messageLogOffset = messageLogOffset;
        this.actionLogOffset = actionLogOffset;
    }

    public int getSyncType() {
        return syncType;
    }

    public long getMessageLogOffset() {
        return messageLogOffset;
    }

    public long getActionLogOffset() {
        return actionLogOffset;
    }

    @Override
    public String toString() {
        return "SyncRequest{" +
                "messageLogOffset=" + messageLogOffset +
                ", actionLogOffset=" + actionLogOffset +
                ", syncType=" + syncType +
                '}';
    }
}
