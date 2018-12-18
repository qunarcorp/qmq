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

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-11-2.
 */
abstract class AbstractPullEntry {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPullEntry.class);

    private static final int MAX_MESSAGE_RETRY_THRESHOLD = 5;

    private final PullService pullService;

    final BrokerService brokerService;
    final AckService ackService;

    private final AtomicReference<Integer> pullRequestTimeout;

    final WeightLoadBalance loadBalance;

    private final QmqCounter pullWorkCounter;
    private final QmqCounter pullFailCounter;

    AbstractPullEntry(String subject, String group, PullService pullService, AckService ackService, BrokerService brokerService) {
        this.pullService = pullService;
        this.ackService = ackService;
        this.brokerService = brokerService;
        this.loadBalance = new WeightLoadBalance();

        pullRequestTimeout = PullSubjectsConfig.get().getPullRequestTimeout(subject);

        String[] values = new String[]{subject, group};
        this.pullWorkCounter = Metrics.counter("qmq_pull_work_count", SUBJECT_GROUP_ARRAY, values);
        this.pullFailCounter = Metrics.counter("qmq_pull_fail_count", SUBJECT_GROUP_ARRAY, values);
    }

    protected List<PulledMessage> pull(ConsumeParam consumeParam, BrokerGroupInfo group, int pullSize, int pullTimeout, AckHook ackHook) {
        pullWorkCounter.inc();
        AckSendInfo ackSendInfo = ackService.getAckSendInfo(group, consumeParam.getSubject(), consumeParam.getGroup());
        final PullParam pullParam = buildPullParam(consumeParam, group, ackSendInfo, pullSize, pullTimeout);
        try {
            PullResult pullResult = pullService.pull(pullParam);
            List<PulledMessage> pulledMessages = handlePullResult(pullParam, pullResult, ackHook);
            group.markSuccess();
            recordPullSize(group, pulledMessages, pullSize);
            return pulledMessages;
        } catch (ExecutionException e) {
            markFailed(group);
            Throwable cause = e.getCause();
            //超时异常暂时不打印日志了
            if (cause instanceof TimeoutException) return Collections.emptyList();
            LOGGER.error("pull message exception. {}", pullParam, e);
        } catch (Exception e) {
            markFailed(group);
            LOGGER.error("pull message exception. {}", pullParam, e);
        }
        return Collections.emptyList();
    }

    private void markFailed(BrokerGroupInfo group) {
        pullFailCounter.inc();
        group.markFailed();
        loadBalance.timeout(group);
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

    private PullParam buildPullParam(ConsumeParam consumeParam, BrokerGroupInfo pullBrokerGroup, AckSendInfo ackSendInfo, int pullSize, int pullTimeout) {
        return new PullParam.PullParamBuilder()
                .setConsumeParam(consumeParam)
                .setBrokerGroup(pullBrokerGroup)
                .setPullBatchSize(pullSize)
                .setTimeoutMillis(pullTimeout)
                .setRequestTimeoutMillis(pullRequestTimeout.get())
                .setMinPullOffset(ackSendInfo.getMinPullOffset())
                .setMaxPullOffset(ackSendInfo.getMaxPullOffset())
                .create();
    }

    private List<PulledMessage> handlePullResult(final PullParam pullParam, final PullResult pullResult, final AckHook ackHook) {
        if (pullResult.getResponseCode() == CommandCode.BROKER_REJECT) {
            pullResult.getBrokerGroup().setAvailable(false);
            brokerService.refresh(ClientType.CONSUMER, pullParam.getSubject(), pullParam.getGroup());
        }

        List<BaseMessage> messages = pullResult.getMessages();
        if (messages != null && !messages.isEmpty()) {
            monitorMessageCount(pullParam, pullResult);
            PulledMessageFilter filter = new PulledMessageFilterImpl(pullParam);
            List<PulledMessage> pulledMessages = ackService.buildPulledMessages(pullParam, pullResult, ackHook, filter);
            if (pulledMessages == null || pulledMessages.isEmpty()) {
                return Collections.emptyList();
            }
            logTimes(pulledMessages);
            return pulledMessages;
        }
        return Collections.emptyList();
    }

    private void logTimes(List<PulledMessage> pulledMessages) {
        for (PulledMessage pulledMessage : pulledMessages) {
            int times = pulledMessage.times();
            if (times > MAX_MESSAGE_RETRY_THRESHOLD) {
                LOGGER.warn("这是第 {} 次收到同一条消息，请注意检查逻辑是否有问题. subject={}, msgId={}",
                        times, RetrySubjectUtils.getRealSubject(pulledMessage.getSubject()), pulledMessage.getMessageId());
            }
        }
    }

    private static void monitorMessageCount(final PullParam pullParam, final PullResult pullResult) {
        try {
            Metrics.counter("qmq_pull_message_count", new String[]{"subject", "group", "broker"},
                    new String[]{pullParam.getSubject(), pullParam.getGroup(), pullParam.getBrokerGroup().getGroupName()})
                    .inc(pullResult.getMessages().size());
        } catch (Exception e) {
            LOGGER.error("AbstractPullEntry monitor exception", e);
        }
    }

    private static final class PulledMessageFilterImpl implements PulledMessageFilter {
        private final PullParam pullParam;

        PulledMessageFilterImpl(PullParam pullParam) {
            this.pullParam = pullParam;
        }

        @Override
        public boolean filter(PulledMessage message) {
            if (pullParam.isConsumeMostOnce() && message.times() > 1) return false;

            //反序列化失败，跳过这个消息
            if (message.getBooleanProperty(BaseMessage.keys.qmq_corruptData.name())) return false;

            // qmq_consumerGroupName
            String group = message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName);
            return Strings.isNullOrEmpty(group) || group.equals(pullParam.getGroup());
        }

    }
}
