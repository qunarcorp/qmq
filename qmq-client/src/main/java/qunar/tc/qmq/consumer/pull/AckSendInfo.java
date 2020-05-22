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
 * @author yiqun.fan create on 17-8-20.
 */
class AckSendInfo {
    private int toSendNum = 0;
    private long minPullOffset = -1;
    private long maxPullOffset = -1;

    public int getToSendNum() {
        return toSendNum;
    }

    public void setToSendNum(int toSendNum) {
        this.toSendNum = toSendNum;
    }

    public long getMinPullOffset() {
        return minPullOffset;
    }

    public void setMinPullOffset(long minPullOffset) {
        this.minPullOffset = minPullOffset;
    }

    public long getMaxPullOffset() {
        return maxPullOffset;
    }

    public void setMaxPullOffset(long maxPullOffset) {
        this.maxPullOffset = maxPullOffset;
    }

    @Override
    public String toString() {
        return "(" + minPullOffset + ", " + maxPullOffset + ", " + toSendNum + ")";
    }
}
