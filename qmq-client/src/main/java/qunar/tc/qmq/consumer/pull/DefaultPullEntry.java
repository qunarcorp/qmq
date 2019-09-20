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
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.SwitchWaiter;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.metainfoclient.DefaultConsumerOnlineStateManager;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

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

    static final DefaultPullEntry EMPTY_PULL_ENTRY = new DefaultPullEntry() {
        @Override
        public void online(StatusSource src) {
        }

        @Override
        public void offline(StatusSource src) {
        }

        @Override
        public void startPull(ExecutorService executor) {
        }
    };

    private static final long PAUSETIME_OF_CLEAN_LAST_MESSAGE = 200;
    private static final long PAUSETIME_OF_NOAVAILABLE_BROKER = 100;
    private static final long PAUSETIME_OF_NOMESSAGE = 500;

    private final ConsumeMessageExecutor consumeMessageExecutor;
    private final AtomicReference<Integer> pullBatchSize;
    private final AtomicReference<Integer> pullTimeout;
    private final AtomicReference<Integer> ackNosendLimit;

    private final Set<String> brokersOfWaitAck = new HashSet<>();

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final SwitchWaiter onlineSwitcher = new SwitchWaiter(false);
    private final ConsumerOnlineStateManager consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();
    private final QmqCounter pullRunCounter;
    private final QmqCounter pauseCounter;
    private final PullStrategy pullStrategy;
    private final ConsumeParam consumeParam;

    private DefaultPullEntry() {
        super("", "", "", "", null, -1, -1, false, false, null, null, null, null);
        consumeMessageExecutor = null;
        pullBatchSize = pullTimeout = ackNosendLimit = null;
        pullRunCounter = null;
        pauseCounter = null;
        pullStrategy = null;
        consumeParam = null;
    }

    DefaultPullEntry(ConsumeMessageExecutor consumeMessageExecutor,
                     ConsumeParam consumeParam,
                     String partitionName,
                     String brokerGroup,
                     ConsumeStrategy consumeStrategy,
                     int version,
                     long consumptionExpiredTime,
                     PullService pullService,
                     AckService ackService,
                     MetaInfoService metaInfoService,
                     BrokerService brokerService,
                     PullStrategy pullStrategy,
                     SendMessageBack sendMessageBack) {
        super(consumeParam.getSubject(), consumeParam.getConsumerGroup(), partitionName, brokerGroup, consumeStrategy, version, consumptionExpiredTime, consumeParam.isBroadcast(), consumeParam.isOrdered(), pullService, ackService, brokerService, sendMessageBack);
        this.consumeParam = consumeParam;
        String subject = consumeParam.getSubject();
        String consumerGroup = consumeParam.getConsumerGroup();

        this.consumeMessageExecutor = consumeMessageExecutor;
        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(subject);
        this.pullTimeout = PullSubjectsConfig.get().getPullTimeout(subject);
        this.ackNosendLimit = PullSubjectsConfig.get().getAckNosendLimit(subject);
        this.pullStrategy = pullStrategy;

        String[] values = new String[]{subject, consumerGroup};
        this.pullRunCounter = Metrics.counter("qmq_pull_run_count", SUBJECT_GROUP_ARRAY, values);
        this.pauseCounter = Metrics.counter("qmq_pull_pause_count", SUBJECT_GROUP_ARRAY, values);

        this.consumerOnlineStateManager.registerConsumer(subject, consumerGroup, consumeParam.getConsumerId(), onlineSwitcher::isOnline);
        this.onlineSwitcher.addListener(isOnline -> {
            if (metaInfoService != null) {
                // 上下线主动触发心跳
                MetaInfoRequest request = new MetaInfoRequest(
                        subject,
                        consumerGroup,
                        ClientType.CONSUMER.getCode(),
                        brokerService.getAppCode(),
                        consumeParam.getConsumerId(),
                        ClientRequestType.SWITCH_STATE,
                        consumeParam.isBroadcast(),
                        consumeParam.isOrdered()
                );
                metaInfoService.request(request);
            }
        });
    }

    @Override
    public void setConsumptionExpiredTime(long timestamp) {
        super.setConsumptionExpiredTime(timestamp);
        this.consumeMessageExecutor.setConsumptionExpiredTime(timestamp);
    }

    public void online(StatusSource src) {
        onlineSwitcher.on(src);
        LOGGER.info("pullconsumer online. subject={}, consumerGroup={}", consumeParam.getSubject(), consumeParam.getConsumerGroup());
    }

    public void offline(StatusSource src) {
        onlineSwitcher.off(src);
        LOGGER.info("pullconsumer offline. subject={}, consumerGroup={}", consumeParam.getSubject(), consumeParam.getConsumerGroup());
        super.offline(src);
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

                    if (isRunning.get() && onlineSwitcher.waitOn()) {
                        // 到这里一定以及收到 metaInfoResponse 且已经上线
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
        if (!consumeMessageExecutor.isFull()) {
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