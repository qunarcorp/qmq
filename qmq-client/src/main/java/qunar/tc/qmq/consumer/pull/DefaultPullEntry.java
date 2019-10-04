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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class DefaultPullEntry extends AbstractPullEntry {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullEntry.class);

    private static final long PAUSETIME_OF_CLEAN_LAST_MESSAGE = 200;
    private static final long PAUSETIME_OF_NOAVAILABLE_BROKER = 100;
    private static final long PAUSETIME_OF_NOMESSAGE = 500;

    private final ConsumeMessageExecutor consumeMessageExecutor;
    private final ConsumerOnlineStateManager consumerOnlineStateManager;
    private final AtomicReference<Integer> pullBatchSize;
    private final AtomicReference<Integer> pullTimeout;
    private final AtomicReference<Integer> ackNosendLimit;

    private final Set<String> brokersOfWaitAck = new HashSet<>();

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final QmqCounter pullRunCounter;
    private final QmqCounter pauseCounter;
    private final PullStrategy pullStrategy;
    private final ConsumeParam consumeParam;

    DefaultPullEntry(ConsumeMessageExecutor consumeMessageExecutor,
                     ConsumeParam consumeParam,
                     String partitionName,
                     String brokerGroup,
                     String consumerId,
                     ConsumeStrategy consumeStrategy,
                     int version,
                     long consumptionExpiredTime,
                     PullService pullService,
                     AckService ackService,
                     BrokerService brokerService,
                     PullStrategy pullStrategy,
                     SendMessageBack sendMessageBack,
                     ConsumerOnlineStateManager consumerOnlineStateManager) {
        super(consumeParam.getSubject(), consumeParam.getConsumerGroup(), partitionName, brokerGroup, consumerId, consumeStrategy, version, consumeParam.isBroadcast(), consumeParam.isOrdered(), consumptionExpiredTime, pullService, ackService, brokerService, sendMessageBack);
        this.consumeParam = consumeParam;
        this.consumerOnlineStateManager = consumerOnlineStateManager;
        this.consumeMessageExecutor = consumeMessageExecutor;
        this.pullStrategy = pullStrategy;

        String subject = consumeParam.getSubject();
        String consumerGroup = consumeParam.getConsumerGroup();
        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(subject);
        this.pullTimeout = PullSubjectsConfig.get().getPullTimeout(subject);
        this.ackNosendLimit = PullSubjectsConfig.get().getAckNosendLimit(subject);

        String[] values = new String[]{subject, consumerGroup};
        this.pullRunCounter = Metrics.counter("qmq_pull_run_count", SUBJECT_GROUP_ARRAY, values);
        this.pauseCounter = Metrics.counter("qmq_pull_pause_count", SUBJECT_GROUP_ARRAY, values);

    }

    @Override
    public void setConsumptionExpiredTime(long timestamp) {
        super.setConsumptionExpiredTime(timestamp);
        this.consumeMessageExecutor.setConsumptionExpiredTime(timestamp);
    }

    @Override
    public void stopPull() {
        isRunning.set(false);
    }

    @Override
    public void startPull(ExecutorService executor) {
        executor.submit(() -> {
            final DoPullParam doPullParam = new DoPullParam();

            while (isRunning.get()) {
                try {
                    if (!preparePull()) {
                        LOGGER.debug("preparePull false. subject={}, consumerGroup={}", consumeParam.getSubject(), consumeParam.getConsumerGroup());
                        continue;
                    }

                    if (!resetDoPullParam(doPullParam)) {
                        LOGGER.debug("buildDoPullParam false. subject={}, consumerGroup={}", consumeParam.getSubject(), consumeParam.getConsumerGroup());
                        continue;
                    }

                    SwitchWaiter switchWaiter = consumerOnlineStateManager.getSwitchWaiter(getSubject(), getConsumerGroup());
                    if (isRunning.get() && switchWaiter.waitOn()) {
                        // 到这里一定已经收到 metaInfoResponse 且已经上线
                        // 因为 onlineSwitcher 已经 online
                        doPull(doPullParam);
                    }
                } catch (Exception e) {
                    LOGGER.error("PullEntry run exception", e);
                }
            }
        });
    }

    private boolean preparePull() {
        pullRunCounter.inc();
        if (consumeMessageExecutor.isFull()) {
            pause("wait consumer", PAUSETIME_OF_CLEAN_LAST_MESSAGE);
            return false;
        }

        if (!pullStrategy.needPull()) {
            pause("wait consumer", PAUSETIME_OF_NOMESSAGE);
            return false;
        }
        return true;
    }

    private boolean resetDoPullParam(DoPullParam param) {
        while (isRunning.get()) {
            BrokerClusterInfo brokerCluster = getBrokerCluster();
            param.brokerGroup = brokerCluster.getGroupByName(getBrokerGroup());
            if (BrokerGroupInfo.isInvalid(param.brokerGroup)) {
                brokersOfWaitAck.clear();
                pause("no available broker", PAUSETIME_OF_NOAVAILABLE_BROKER);
                continue;
            }

            param.ackSendInfo = getAckSendQueue().getAckSendInfo();
            if (param.ackSendInfo.getToSendNum() <= ackNosendLimit.get()) {
                brokersOfWaitAck.clear();
                break;
            }
            param.ackSendInfo = null;
            brokersOfWaitAck.add(param.brokerGroup.getGroupName());
        }
        return isRunning.get() && param.ackSendInfo != null;
    }

    private BrokerClusterInfo getBrokerCluster() {
        return brokerService.getConsumerBrokerCluster(ClientType.CONSUMER, consumeParam.getSubject());
    }

    private void doPull(DoPullParam param) {
        List<PulledMessage> messages = pull(consumeParam, param.brokerGroup, pullBatchSize.get(), pullTimeout.get(), consumeMessageExecutor.getMessageHandler());
        pullStrategy.record(messages.size() > 0);
        consumeMessageExecutor.consume(messages);
    }

    private void pause(String log, long timeMillis) {
        final String subject = consumeParam.getSubject();
        final String consumerGroup = consumeParam.getConsumerGroup();
        this.pauseCounter.inc();
        LOGGER.debug("pull pause {} ms, {}. subject={}, consumerGroup={}", timeMillis, log, subject, consumerGroup);
        try {
            Thread.sleep(timeMillis);
        } catch (Exception e) {
            LOGGER.info("PullEntry pause exception. log={}", log, e);
        }
    }

    private static final class DoPullParam {
        private volatile AckSendInfo ackSendInfo = null;
        private volatile BrokerGroupInfo brokerGroup = null;
    }
}