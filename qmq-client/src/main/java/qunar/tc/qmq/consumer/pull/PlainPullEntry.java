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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;

/**
 * @author yiqun.fan create on 17-9-21.
 */
class PlainPullEntry extends AbstractPullEntry {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlainPullEntry.class);

    private final BrokerService brokerService;
    private final ConsumeParam consumeParam;
    private final PullStrategy pullStrategy;
    private final WeightLoadBalance loadBalance;

    PlainPullEntry(
            ConsumeParam consumeParam,
            String partitionName,
            String brokerGroup,
            String consumerId,
            ConsumeStrategy consumeStrategy,
            int allocationVersion,
            long consumptionExpiredTime,
            PullService pullService,
            AckService ackService,
            BrokerService brokerService,
            PullStrategy pullStrategy,
            SendMessageBack sendMessageBack) {
        super(
                consumeParam.getSubject(),
                consumeParam.getConsumerGroup(),
                partitionName,
                brokerGroup,
                consumerId,
                consumeStrategy,
                allocationVersion,
                consumeParam.isBroadcast(),
                consumeParam.isOrdered(),
                consumptionExpiredTime,
                pullService,
                ackService,
                brokerService,
                sendMessageBack);
        this.consumeParam = consumeParam;
        this.pullStrategy = pullStrategy;
        this.brokerService = brokerService;
        this.loadBalance = new WeightLoadBalance();
    }

    PlainPullResult pull(final int fetchSize, final int pullTimeout, final List<Message> output) {
        if (!pullStrategy.needPull()) {
            return PlainPullResult.NOMORE_MESSAGE;
        }
        BrokerClusterInfo brokerCluster = brokerService
                .getConsumerBrokerCluster(ClientType.CONSUMER, consumeParam.getSubject());
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

    protected void markFailed(BrokerGroupInfo group) {
        super.markFailed(group);
        loadBalance.timeout(group);
    }

    protected List<PulledMessage> pull(ConsumeParam consumeParam, BrokerGroupInfo brokerGroupInfo, int pullSize,
            int pullTimeout, AckHook ackHook) {
        pullWorkCounter.inc();
        AckSendInfo ackSendInfo = ackSendQueue.getAckSendInfo();
        final PullParam pullParam = buildPullParam(consumeParam, brokerGroupInfo, ackSendInfo, pullSize, pullTimeout);
        try {
            PullResult pullResult = pullService.pull(pullParam);
            List<PulledMessage> pulledMessages = handlePullResult(pullParam, pullResult, ackHook);
            brokerGroupInfo.markSuccess();
            recordPullSize(brokerGroupInfo, pulledMessages, pullSize);
            return pulledMessages;
        } catch (ExecutionException e) {
            markFailed(brokerGroupInfo);
            Throwable cause = e.getCause();
            //超时异常暂时不打印日志了
            if (cause instanceof TimeoutException) {
                return Collections.emptyList();
            }
            LOGGER.error("pull message exception. {}", pullParam, e);
        } catch (Exception e) {
            markFailed(brokerGroupInfo);
            LOGGER.error("pull message exception. {}", pullParam, e);
        }
        return Collections.emptyList();
    }

    private void recordPullSize(BrokerGroupInfo group, List<PulledMessage> received, int pullSize) {
        if (received.size() == 0) {
            loadBalance.noMessage(group);
            return;
        }

        if (received.size() >= pullSize) {
            loadBalance.fetchedEnoughMessages(group);
            return;
        }

        loadBalance.fetchedMessages(group);
    }

    @Override
    public void startPull(ExecutorService executor) {

    }

    @Override
    public void stopPull() {

    }

    enum PlainPullResult {
        NO_BROKER,
        NOMORE_MESSAGE,
        COMPLETE,
        REQUESTING
    }
}
