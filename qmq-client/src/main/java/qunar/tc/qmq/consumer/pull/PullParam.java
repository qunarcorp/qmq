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

import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.protocol.consumer.PullFilter;

import java.util.List;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class PullParam {
    private final ConsumeParam consumeParam;
    private final BrokerGroupInfo brokerGroup;
    private final int pullBatchSize;
    private final long timeoutMillis;
    private final long requestTimeoutMillis;
    private final long consumeOffset;
    private final long minPullOffset;
    private final long maxPullOffset;

    private PullParam(ConsumeParam consumeParam, BrokerGroupInfo brokerGroup,
                      int pullBatchSize, long timeoutMillis, long requestTimeoutMillis,
                      long consumeOffset, long minPullOffset, long maxPullOffset) {
        this.consumeParam = consumeParam;
        this.brokerGroup = brokerGroup;
        this.pullBatchSize = pullBatchSize;
        this.timeoutMillis = timeoutMillis;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.consumeOffset = consumeOffset;
        this.minPullOffset = minPullOffset;
        this.maxPullOffset = maxPullOffset;
    }

    public String getSubject() {
        return consumeParam.getSubject();
    }

    public String getGroup() {
        return consumeParam.getGroup();
    }

    public String getOriginSubject() {
        return consumeParam.getOriginSubject();
    }

    public BrokerGroupInfo getBrokerGroup() {
        return brokerGroup;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public long getConsumeOffset() {
        return consumeOffset;
    }

    public long getMinPullOffset() {
        return minPullOffset;
    }

    public long getMaxPullOffset() {
        return maxPullOffset;
    }

    public String getConsumerId() {
        return consumeParam.getConsumerId();
    }

    public boolean isBroadcast() {
        return consumeParam.isBroadcast();
    }

    public boolean isConsumeMostOnce() {
        return consumeParam.isConsumeMostOnce();
    }

    public List<PullFilter> getFilters() {
        return consumeParam.getFilters();
    }

    @Override
    public String toString() {
        return "PullParam{" +
                "consumeParam=" + consumeParam +
                ", brokerGroup=" + brokerGroup +
                ", pullBatchSize=" + pullBatchSize +
                ", timeoutMillis=" + timeoutMillis +
                ", consumeOffset=" + consumeOffset +
                ", minPullOffset=" + minPullOffset +
                ", maxPullOffset=" + maxPullOffset +
                '}';
    }

    public static final class PullParamBuilder {
        private ConsumeParam consumeParam;
        private BrokerGroupInfo brokerGroup;
        private int pullBatchSize;
        private long timeoutMillis;
        private long requestTimeoutMillis;
        private long consumeOffset = -1;
        private long minPullOffset = -1;
        private long maxPullOffset = -1;

        public PullParam create() {
            return new PullParam(consumeParam, brokerGroup, pullBatchSize, timeoutMillis, requestTimeoutMillis, consumeOffset, minPullOffset, maxPullOffset);
        }

        public PullParamBuilder setConsumeParam(ConsumeParam consumeParam) {
            this.consumeParam = consumeParam;
            return this;
        }

        public PullParamBuilder setBrokerGroup(BrokerGroupInfo brokerGroup) {
            this.brokerGroup = brokerGroup;
            return this;
        }

        public PullParamBuilder setPullBatchSize(int pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
            return this;
        }

        public PullParamBuilder setTimeoutMillis(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        public PullParamBuilder setRequestTimeoutMillis(long requestTimeoutMillis) {
            this.requestTimeoutMillis = requestTimeoutMillis;
            return this;
        }

        public PullParamBuilder setConsumeOffset(long consumeOffset) {
            this.consumeOffset = consumeOffset;
            return this;
        }

        public PullParamBuilder setMinPullOffset(long minPullOffset) {
            this.minPullOffset = minPullOffset;
            return this;
        }

        public PullParamBuilder setMaxPullOffset(long maxPullOffset) {
            this.maxPullOffset = maxPullOffset;
            return this;
        }
    }
}
