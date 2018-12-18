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

import qunar.tc.qmq.Message;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;

import java.util.List;

/**
 * @author yiqun.fan create on 17-9-21.
 */
class PlainPullEntry extends AbstractPullEntry {

    private final ConsumeParam consumeParam;
    private final PullStrategy pullStrategy;

    PlainPullEntry(ConsumeParam consumeParam, PullService pullService, AckService ackService, BrokerService brokerService, PullStrategy pullStrategy) {
        super(consumeParam.getSubject(), consumeParam.getGroup(), pullService, ackService, brokerService);
        this.consumeParam = consumeParam;
        this.pullStrategy = pullStrategy;
    }

    PlainPullResult pull(final int fetchSize, final int pullTimeout, final List<Message> output) {
        if (!pullStrategy.needPull()) return PlainPullResult.NOMORE_MESSAGE;
        BrokerClusterInfo brokerCluster = brokerService.getClusterBySubject(ClientType.CONSUMER, consumeParam.getSubject(), consumeParam.getGroup());
        List<BrokerGroupInfo> groups = brokerCluster.getGroups();
        if (groups.isEmpty()) {
            return PlainPullResult.NO_BROKER;
        }
        BrokerGroupInfo group = loadBalance.select(brokerCluster);
        List<PulledMessage> received = pull(consumeParam, group, fetchSize, pullTimeout, null);
        output.addAll(received);
        pullStrategy.record(received.size() > 0);
        return PlainPullResult.NOMORE_MESSAGE;
    }

    enum PlainPullResult {
        NO_BROKER,
        NOMORE_MESSAGE,
        COMPLETE,
        REQUESTING
    }
}
