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
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.StatusSource;
import qunar.tc.qmq.common.SwitchWaiter;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.metainfoclient.DefaultConsumerOnlineStateManager;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
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

    private final PushConsumer pushConsumer;
    private final AtomicReference<Integer> pullBatchSize;
    private final AtomicReference<Integer> pullTimeout;
    private final AtomicReference<Integer> ackNosendLimit;

    private final Set<String> brokersOfWaitAck = new HashSet<>();

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final SwitchWaiter onlineSwitcher = new SwitchWaiter(false);
    private final ConsumerOnlineStateManager consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();
    private final QmqCounter pullRunCounter;
    private final QmqCounter pauseCounter;
    private final String logType;
    private final MetaInfoService metaInfoService;
    private final PullStrategy pullStrategy;

    private DefaultPullEntry() {
        super("", "", null, null, null);
        metaInfoService = null;
        pushConsumer = null;
        pullBatchSize = pullTimeout = ackNosendLimit = null;
        pullRunCounter = null;
        pauseCounter = null;
        logType = "PullEntry=";
        pullStrategy = null;
    }

    DefaultPullEntry(PushConsumer pushConsumer, PullService pullService, AckService ackService, MetaInfoService metaInfoService, BrokerService brokerService, PullStrategy pullStrategy) {
        super(pushConsumer.subject(), pushConsumer.group(), pullService, ackService, brokerService);
        this.metaInfoService = metaInfoService;
        String subject = pushConsumer.subject();
        String group = pushConsumer.group();
        this.pushConsumer = pushConsumer;
        String realSubject = RetrySubjectUtils.getRealSubject(subject);
        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(realSubject);
        this.pullTimeout = PullSubjectsConfig.get().getPullTimeout(realSubject);
        this.ackNosendLimit = PullSubjectsConfig.get().getAckNosendLimit(realSubject);
        this.pullStrategy = pullStrategy;

        String[] values = new String[]{subject, group};
        this.pullRunCounter = Metrics.counter("qmq_pull_run_count", SUBJECT_GROUP_ARRAY, values);
        this.pauseCounter = Metrics.counter("qmq_pull_pause_count", SUBJECT_GROUP_ARRAY, values);

        this.logType = "PullEntry=" + subject;

        this.consumerOnlineStateManager.registerConsumer(subject, group, pushConsumer.consumeParam().getConsumerId(), onlineSwitcher::isOnline);
        this.onlineSwitcher.addListener(isOnline -> {
            if (metaInfoService != null) {
                // 上下线主动触发心跳
                metaInfoService.triggerConsumerMetaInfoRequest(ClientRequestType.SWITCH_STATE);
            }
        });
    }

    public void online(StatusSource src) {
        onlineSwitcher.on(src);
        LOGGER.info("pullconsumer online. subject={}, group={}", pushConsumer.subject(), pushConsumer.group());
    }

    public void offline(StatusSource src) {
        onlineSwitcher.off(src);
        LOGGER.info("pullconsumer offline. subject={}, group={}", pushConsumer.subject(), pushConsumer.group());
    }

    public void destroy() {
        isRunning.set(false);
    }

    @Override
    public void startPull(ExecutorService executor) {
        executor.submit(() -> {
            final DoPullParam doPullParam = new DoPullParam();

            while (isRunning.get()) {
                try {
                    if (!preparePull()) {
                        LOGGER.debug(logType, "preparePull false. subject={}, group={}", pushConsumer.subject(), pushConsumer.group());
                        continue;
                    }

                    if (!resetDoPullParam(doPullParam)) {
                        LOGGER.debug(logType, "buildDoPullParam false. subject={}, group={}", pushConsumer.subject(), pushConsumer.group());
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
        if (!pushConsumer.cleanLocalBuffer()) {
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
            param.cluster = getBrokerCluster();
            param.broker = nextPullBrokerGroup(param.cluster);
            if (BrokerGroupInfo.isInvalid(param.broker)) {
                brokersOfWaitAck.clear();
                pause("noavaliable broker", PAUSETIME_OF_NOAVAILABLE_BROKER);
                continue;
            }

            param.ackSendInfo = ackService.getAckSendInfo(param.broker, pushConsumer.subject(), pushConsumer.group());
            if (param.ackSendInfo.getToSendNum() <= ackNosendLimit.get()) {
                brokersOfWaitAck.clear();
                break;
            }
            param.ackSendInfo = null;
            brokersOfWaitAck.add(param.broker.getGroupName());
        }
        return isRunning.get() && param.ackSendInfo != null;
    }

    private BrokerClusterInfo getBrokerCluster() {
        return brokerService.getClusterBySubject(ClientType.CONSUMER, pushConsumer.subject(), pushConsumer.group());
    }

    private BrokerGroupInfo nextPullBrokerGroup(BrokerClusterInfo cluster) {
        //没有分配到brokers
        List<BrokerGroupInfo> groups = cluster.getGroups();
        if (groups.isEmpty()) return null;

        final int brokerSize = groups.size();
        for (int cnt = 0; cnt < brokerSize; cnt++) {
            BrokerGroupInfo broker = loadBalance.select(cluster);
            if (broker == null) return null;
            if (brokersOfWaitAck.contains(broker.getGroupName())) continue;

            return broker;
        }
        // 没有可用的brokers
        return null;
    }

    private void doPull(DoPullParam param) {
        List<PulledMessage> messages = pull(pushConsumer.consumeParam(), param.broker, pullBatchSize.get(), pullTimeout.get(), pushConsumer);
        pullStrategy.record(messages.size() > 0);
        pushConsumer.push(messages);
    }

    private void pause(String log, long timeMillis) {
        final String subject = pushConsumer.subject();
        final String group = pushConsumer.group();
        this.pauseCounter.inc();
        LOGGER.debug(logType, "pull pause {} ms, {}. subject={}, group={}", timeMillis, log, subject, group);
        try {
            Thread.sleep(timeMillis);
        } catch (Exception e) {
            LOGGER.info("PullEntry pause exception. log={}", log, e);
        }
    }

    private static final class DoPullParam {
        private volatile BrokerClusterInfo cluster = null;
        private volatile BrokerGroupInfo broker = null;
        private volatile AckSendInfo ackSendInfo = null;
    }
}