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

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author yiqun.fan create on 17-9-21.
 */
class PlainPullEntry extends AbstractPullEntry {

    private final ConsumeParam consumeParam;
    private final PullStrategy pullStrategy;

    PlainPullEntry(
            ConsumeParam consumeParam,
            String partitionName,
            String brokerGroup,
            ConsumeStrategy consumeStrategy,
            int allocationVersion,
            long consumptionExpiredTime,
            PullService pullService,
            AckService ackService,
            BrokerService brokerService,
            PullStrategy pullStrategy,
            SendMessageBack sendMessageBack) {
        super(consumeParam.getSubject(), consumeParam.getConsumerGroup(), partitionName, brokerGroup, consumeStrategy, allocationVersion, consumptionExpiredTime, consumeParam.isBroadcast(), consumeParam.isOrdered(), pullService, ackService, brokerService, sendMessageBack);
        this.consumeParam = consumeParam;
        this.pullStrategy = pullStrategy;
    }

    PlainPullResult pull(final int fetchSize, final int pullTimeout, final List<Message> output) {
        if (!pullStrategy.needPull()) return PlainPullResult.NOMORE_MESSAGE;
        BrokerClusterInfo brokerCluster = brokerService.getConsumerBrokerCluster(ClientType.CONSUMER, consumeParam.getSubject());
        List<BrokerGroupInfo> groups = brokerCluster.getGroups();
        if (groups.isEmpty()) {
            return PlainPullResult.NO_BROKER;
        }
        BrokerGroupInfo brokerGroup = loadBalance.select(brokerCluster);
        List<PulledMessage> received = pull(consumeParam, brokerGroup, fetchSize, pullTimeout, null);
        output.addAll(received);
        pullStrategy.record(received.size() > 0);
        return PlainPullResult.NOMORE_MESSAGE;
    }

    @Override
    public void startPull(ExecutorService executor) {

    }

    @Override
    public void stopPull() {

    }

    @Override
    public void destroy() {
        super.destroy();
    }

    @Override
    public void online(StatusSource statusSource) {

    }

    @Override
    public void offline(StatusSource statusSource) {
        super.offline(statusSource);
    }

    enum PlainPullResult {
        NO_BROKER,
        NOMORE_MESSAGE,
        COMPLETE,
        REQUESTING
    }
}
