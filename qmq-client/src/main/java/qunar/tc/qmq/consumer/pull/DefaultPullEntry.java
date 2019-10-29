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
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.StatusSource;
import qunar.tc.qmq.common.SwitchWaiter;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class DefaultPullEntry extends AbstractPullEntry {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullEntry.class);

    private static final long PAUSETIME_OF_CLEAN_LAST_MESSAGE = 200;
    private static final long PAUSETIME_OF_NOAVAILABLE_BROKER = 100;
    private static final long PAUSETIME_OF_NOMESSAGE = 500;

    private final PushConsumer pushConsumer;
    private final AtomicReference<Integer> pullBatchSize;
    private final AtomicReference<Integer> pullTimeout;
    private final AtomicReference<Integer> ackNosendLimit;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final SwitchWaiter onlineSwitcher;
    private final QmqCounter pullRunCounter;
    private final QmqCounter pauseCounter;
    private final String logType;
    private final PullStrategy pullStrategy;
    private final String brokerGroupName;

    DefaultPullEntry(String brokerGroupName, PushConsumer pushConsumer, PullService pullService, AckService ackService,
                     BrokerService brokerService, PullStrategy pullStrategy, SwitchWaiter switchWaiter) {
        super(pushConsumer.subject(), pushConsumer.group(), pullService, ackService, brokerService);
        this.brokerGroupName = brokerGroupName;
        String subject = pushConsumer.subject();
        String group = pushConsumer.group();
        this.pushConsumer = pushConsumer;
        String realSubject = RetrySubjectUtils.getRealSubject(subject);
        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(realSubject);
        this.pullTimeout = PullSubjectsConfig.get().getPullTimeout(realSubject);
        this.ackNosendLimit = PullSubjectsConfig.get().getAckNosendLimit(realSubject);
        this.pullStrategy = pullStrategy;
        this.onlineSwitcher = switchWaiter;

        String[] values = new String[]{subject, group};
        this.pullRunCounter = Metrics.counter("qmq_pull_run_count", SUBJECT_GROUP_ARRAY, values);
        this.pauseCounter = Metrics.counter("qmq_pull_pause_count", SUBJECT_GROUP_ARRAY, values);

        this.logType = "PullEntry=" + subject;
    }

    @Override
    public String getSubject() {
        return pushConsumer.subject();
    }

    @Override
    public String getConsumerGroup() {
        return pushConsumer.group();
    }

    public void online(StatusSource src) {
    }

    public void offline(StatusSource src) {
    }

    public void destroy() {
        isRunning.set(false);
    }

    @Override
    public void startPull(Executor executor) {
        executor.execute(this::pullInLoop);
    }

    private void pullInLoop() {
        final DoPullParam doPullParam = new DoPullParam();

        while (isRunning.get()) {
            try {
                if (!preparePull()) {
                    LOGGER.debug(logType, "preparePull false. subject={}, group={}", pushConsumer.subject(),
                            pushConsumer.group());
                    continue;
                }

                if (!resetDoPullParam(doPullParam)) {
                    LOGGER.debug(logType, "buildDoPullParam false. subject={}, group={}", pushConsumer.subject(),
                            pushConsumer.group());
                    continue;
                }

                if (isRunning.get() && onlineSwitcher.waitOn()) {
                    doPull(doPullParam);
                }
            } catch (Exception e) {
                LOGGER.error("PullEntry run exception", e);
            }
        }
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
            param.broker = getBrokerGroup();
            if (BrokerGroupInfo.isInvalid(param.broker)) {
                pause("noavaliable broker", PAUSETIME_OF_NOAVAILABLE_BROKER);
                continue;
            }

            param.ackSendInfo = ackService.getAckSendInfo(param.broker, pushConsumer.subject(), pushConsumer.group());
            if (param.ackSendInfo.getToSendNum() > ackNosendLimit.get()) {
                pause("wait ack", PAUSETIME_OF_CLEAN_LAST_MESSAGE);
                continue;
            }

            break;
        }
        return isRunning.get() && param.ackSendInfo != null;
    }

    private BrokerGroupInfo getBrokerGroup() {
        BrokerClusterInfo brokerCluster = getBrokerCluster();
        return brokerCluster.getGroupByName(brokerGroupName);
    }

    private BrokerClusterInfo getBrokerCluster() {
        return brokerService.getClusterBySubject(ClientType.CONSUMER, pushConsumer.subject(), pushConsumer.group());
    }

    private void doPull(DoPullParam param) {
        List<PulledMessage> messages = pull(pushConsumer.consumeParam(), param.broker, pullBatchSize.get(),
                pullTimeout.get(), pushConsumer);
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
        private volatile AckSendInfo ackSendInfo = null;
        private volatile BrokerGroupInfo broker = null;
    }
}
