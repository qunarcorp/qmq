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

package qunar.tc.qmq.protocol.consumer;

import java.util.List;

/**
 * @author yiqun.fan create on 17-7-4.
 */
public abstract class AbstractPullRequest implements PullRequest {
    private String partitionName;
    private String group;
    private int requestNum;
    private long timeoutMillis;
    private long offset;
    private long pullOffsetBegin;
    private long pullOffsetLast;
    private String consumerId;
    private boolean isExclusiveConsume;
    private List<PullFilter> filters;

    public AbstractPullRequest(String partitionName, String group, int requestNum, long timeoutMillis, long offset, long pullOffsetBegin, long pullOffsetLast, String consumerId, boolean isExclusiveConsume, List<PullFilter> filters) {
        this.partitionName = partitionName;
        this.group = group;
        this.requestNum = requestNum;
        this.timeoutMillis = timeoutMillis;
        this.offset = offset;
        this.pullOffsetBegin = pullOffsetBegin;
        this.pullOffsetLast = pullOffsetLast;
        this.consumerId = consumerId;
        this.isExclusiveConsume = isExclusiveConsume;
        this.filters = filters;
    }

    public void setRequestNum(int requestNum) {
        this.requestNum = requestNum;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public void setFilters(List<PullFilter> filters) {
        this.filters = filters;
    }

    public int getRequestNum() {
        return requestNum;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getOffset() {
        return offset;
    }

    public long getPullOffsetBegin() {
        return pullOffsetBegin;
    }

    public long getPullOffsetLast() {
        return pullOffsetLast;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public boolean isExclusiveConsume() {
        return isExclusiveConsume;
    }

    public List<PullFilter> getFilters() {
        return filters;
    }

    @Override
    public String toString() {
        return "PullRequest{" +
                "partitionName='" + partitionName + '\'' +
                ", group='" + group + '\'' +
                ", requestNum=" + requestNum +
                ", timeoutMillis=" + timeoutMillis +
                ", offset=" + offset +
                ", pullOffsetBegin=" + pullOffsetBegin +
                ", pullOffsetLast=" + pullOffsetLast +
                ", consumerId='" + consumerId + '\'' +
                ", isExclusiveConsume=" + isExclusiveConsume +
                ", filters=" + filters +
                '}';
    }
}
