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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.StatusSource;
import qunar.tc.qmq.common.SwitchWaiter;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class DefaultPullEntry extends AbstractPullEntry implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullEntry.class);

    static final ScheduledExecutorService DELAY_SCHEDULER = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-delay-scheduler"));

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
    private final PullStrategy pullStrategy;
    private final String brokerGroupName;

    private volatile SettableFuture<Boolean> onlineFuture;

    private final Executor executor;

    DefaultPullEntry(String brokerGroupName, PushConsumer pushConsumer, PullService pullService, AckService ackService,
                     BrokerService brokerService, PullStrategy pullStrategy, SwitchWaiter switchWaiter, Executor executor) {
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
        this.executor = executor;

        String[] values = new String[]{subject, group};
        this.pullRunCounter = Metrics.counter("qmq_pull_run_count", SUBJECT_GROUP_ARRAY, values);
        this.pauseCounter = Metrics.counter("qmq_pull_pause_count", SUBJECT_GROUP_ARRAY, values);
    }

    @Override
    public void startPull() {
        executor.execute(this);
    }

    private static final int PREPARE_PULL = 0;
    private static final int PULL_DONE = 1;

    private final AtomicInteger state = new AtomicInteger(PREPARE_PULL);

    private volatile PullParam pullParam;
    private volatile PullService.PullResultFuture pullFuture;

    @Override
    public void run() {
        Thread thread = Thread.currentThread();
        String oldThreadName = thread.getName();
        thread.setName("qmq-pull-entry-" + getSubject() + "-" + getConsumerGroup() + "-" + getBrokerGroup());
        try {
            switch (state.get()) {
                case PREPARE_PULL:
                    if (!isRunning.get()) {
                        return;
                    }

                    if (await(waitOnline())) {
                        return;
                    }

                    if (await(preparePull())) {
                        return;
                    }

                    if (await(resetDoPullParam())) {
                        return;
                    }

                    AckSendInfo ackSendInfo = ackService.getAckSendInfo(getBrokerGroup(), getSubject(), getConsumerGroup());
                    pullParam = buildPullParam(pushConsumer.consumeParam(), getBrokerGroup(), ackSendInfo, pullBatchSize.get(), pullTimeout.get());
                    pullFuture = pullService.pullAsync(pullParam);
                    state.set(PULL_DONE);
                    await(pullFuture);
                    break;
                case PULL_DONE:
                    final PullParam thisPullParam = pullParam;
                    try {
                        PullResult pullResult = pullFuture.get();
                        List<PulledMessage> messages = handlePullResult(thisPullParam, pullResult, pushConsumer);
                        getBrokerGroup().markSuccess();
                        pullStrategy.record(messages.size() > 0);
                        pushConsumer.push(messages);
                    } catch (ExecutionException e) {
                        markFailed(getBrokerGroup());
                        Throwable cause = e.getCause();
                        //超时异常暂时不打印日志了
                        if (!(cause instanceof TimeoutException)) {
                            LOGGER.error("pull message exception. {}", thisPullParam, e);
                        }
                    } catch (Exception e) {
                        markFailed(getBrokerGroup());
                        LOGGER.error("pull message exception. {}", thisPullParam, e);
                    } finally {
                        state.set(PREPARE_PULL);
                        run();
                    }
                    break;
            }
        } catch (Throwable t) {
            LOGGER.error("pull error subject {} consumerGroup {}", getSubject(), getConsumerGroup(), t);
        } finally {
            thread.setName(oldThreadName);
        }
    }

    private boolean await(ListenableFuture future) {
        if (future == null) {
            return false;
        }
        future.addListener(this, executor);
        return true;
    }

    private ListenableFuture waitOnline() {
        synchronized (onlineSwitcher) {
            if (onlineSwitcher.isOnline()) {
                return null;
            }

            final SettableFuture<Boolean> future = SettableFuture.create();
            this.onlineFuture = future;
            return future;
        }
    }

    private ListenableFuture preparePull() {
        pullRunCounter.inc();
        if (!pushConsumer.cleanLocalBuffer()) {
            return delay("wait consumer", PAUSETIME_OF_CLEAN_LAST_MESSAGE);
        }

        if (!pullStrategy.needPull()) {
            return delay("wait consumer", PAUSETIME_OF_NOMESSAGE);
        }
        return null;
    }

    private ListenableFuture resetDoPullParam() {
        BrokerGroupInfo brokerGroup = getBrokerGroup();
        if (BrokerGroupInfo.isInvalid(brokerGroup)) {
            return delay("no available broker", PAUSETIME_OF_NOAVAILABLE_BROKER);
        }

        AckSendInfo ackSendInfo = ackService.getAckSendInfo(brokerGroup, pushConsumer.subject(), pushConsumer.group());
        if (ackSendInfo.getToSendNum() > ackNosendLimit.get()) {
            return delay("wait ack", PAUSETIME_OF_NOAVAILABLE_BROKER);
        }
        return null;
    }

    private ListenableFuture delay(String log, long timeMillis) {
        final String subject = getSubject();
        final String consumerGroup = getConsumerGroup();
        this.pauseCounter.inc();
        LOGGER.debug("pull pause {} ms, {}. subject={}, consumerGroup={}", timeMillis, log, subject, consumerGroup);
        RunnableSettableFuture future = new RunnableSettableFuture();
        DELAY_SCHEDULER.schedule(future, timeMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    @Override
    public String getSubject() {
        return pushConsumer.subject();
    }

    @Override
    public String getConsumerGroup() {
        return pushConsumer.group();
    }

    @Override
    public void online(StatusSource src) {
        synchronized (onlineSwitcher) {
            final SettableFuture<Boolean> future = this.onlineFuture;
            if (future == null) return;
            future.set(true);
        }
    }

    @Override
    public void offline(StatusSource src) {
    }

    @Override
    public void destroy() {
        isRunning.set(false);
    }

    private static class RunnableSettableFuture extends AbstractFuture implements Runnable {

        @Override
        public void run() {
            super.set(null);
        }
    }

    private BrokerGroupInfo getBrokerGroup() {
        BrokerClusterInfo brokerCluster = getBrokerCluster();
        return brokerCluster.getGroupByName(brokerGroupName);
    }

    private BrokerClusterInfo getBrokerCluster() {
        return brokerService.getClusterBySubject(ClientType.CONSUMER, pushConsumer.subject(), pushConsumer.group());
    }
}
