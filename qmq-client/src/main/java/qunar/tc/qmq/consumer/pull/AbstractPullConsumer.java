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

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.broker.BrokerService;

import java.util.List;

/**
 * @author yiqun.fan create on 17-10-19.
 */
abstract class AbstractPullConsumer extends AbstractPullClient implements PullConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPullConsumer.class);

    private static final long MIN_PULL_TIMEOUT_MILLIS = 1000;  // 最短拉取超时时间是1秒

    private final ConsumeParam consumeParam;
    final PlainPullEntry pullEntry;

    AbstractPullConsumer(
            String subject,
            String consumerGroup,
            String partitionName,
            String brokerGroup,
            ConsumeStrategy consumeStrategy,
            int allocationVersion,
            long consumptionExpiredTime,
            boolean isBroadcast,
            boolean isOrdered,
            String consumerId,
            PullService pullService,
            AckService ackService,
            BrokerService brokerService,
            int partitionSetVersion,
            SendMessageBack sendMessageBack) {
        super(subject, consumerGroup, partitionName, brokerGroup, consumerId, consumeStrategy, allocationVersion, isBroadcast, isOrdered,
                partitionSetVersion, consumptionExpiredTime);
        this.consumeParam = new ConsumeParam(subject, consumerGroup, isBroadcast, isOrdered, false, consumerId);
        this.pullEntry = new PlainPullEntry(
                consumeParam,
                partitionName,
                brokerGroup,
                getClientId(),
                consumeStrategy,
                allocationVersion,
                consumptionExpiredTime,
                pullService,
                ackService,
                brokerService,
                new AlwaysPullStrategy(),
                partitionSetVersion,
                sendMessageBack);
    }

    private static long checkAndGetTimeout(long timeout) {
        return timeout < 0 ? timeout : Math.min(Math.max(timeout, MIN_PULL_TIMEOUT_MILLIS), MAX_PULL_TIMEOUT_MILLIS);
    }

    @Override
    public void setConsumeMostOnce(boolean consumeMostOnce) {
        consumeParam.setConsumeMostOnce(consumeMostOnce);
    }

    @Override
    public boolean isConsumeMostOnce() {
        return consumeParam.isConsumeMostOnce();
    }

    @Override
    public List<Message> pull(int size) {
        return newFuture(size, MAX_PULL_TIMEOUT_MILLIS, false).get();
    }

    public List<Message> pull(int size, long timeoutMillis) {
        return newFuture(size, checkAndGetTimeout(timeoutMillis), false).get();
    }

    @Override
    public List<Message> pull(int size, long timeoutMillis, boolean waitIfNotComplete) {
        return newFuture(size, checkAndGetTimeout(timeoutMillis), false).get();
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size) {
        return newFuture(size, MAX_PULL_TIMEOUT_MILLIS, false);
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size, long timeoutMillis) {
        return newFuture(size, checkAndGetTimeout(timeoutMillis), DEFAULT_RESET_CREATE_TIME);
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size, long timeoutMillis, boolean isResetCreateTime) {
        return newFuture(size, checkAndGetTimeout(timeoutMillis), isResetCreateTime);
    }

    abstract PullMessageFuture newFuture(int size, long timeout, boolean isResetCreateTime);

    @Override
    public String getClientId() {
        return consumeParam.getConsumerId();
    }

    @Override
    public void destroy() {
        pullEntry.destroy();
        super.destroy();
    }
}
