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

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullEntry;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class DefaultPullEntry extends AbstractPullEntry implements PullEntry, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullEntry.class);

    private static final ScheduledExecutorService DELAY_SCHEDULER = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-delay-scheduler"));

    private static final long PAUSETIME_OF_CLEAN_LAST_MESSAGE = 200;
    private static final long PAUSETIME_OF_NOAVAILABLE_BROKER = 100;
    private static final long PAUSETIME_OF_NOMESSAGE = 500;

    private final ConsumeMessageExecutor consumeMessageExecutor;

    private final AtomicReference<Integer> pullBatchSize;
    private final AtomicReference<Integer> pullTimeout;
    private final AtomicReference<Integer> ackNosendLimit;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    private final AtomicBoolean isOnline = new AtomicBoolean(false);
    private SettableFuture waitOnlineFuture;

    private final QmqCounter pullRunCounter;
    private final QmqCounter pauseCounter;

    private final PullStrategy pullStrategy;
    private final ConsumeParam consumeParam;

    private final ExecutorService partitionExecutor;

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
            ConsumerOnlineStateManager consumerOnlineStateManager,
            ExecutorService partitionExecutor) {
        super(consumeParam.getSubject(), consumeParam.getConsumerGroup(), partitionName, brokerGroup, consumerId,
                consumeStrategy, version, consumeParam.isBroadcast(), consumeParam.isOrdered(), consumptionExpiredTime,
                pullService, ackService, brokerService, sendMessageBack);
        this.partitionExecutor = partitionExecutor;

        String subject = consumeParam.getSubject();
        String consumerGroup = consumeParam.getConsumerGroup();

        this.consumeParam = consumeParam;
        this.consumeMessageExecutor = consumeMessageExecutor;
        this.pullStrategy = pullStrategy;

        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(subject);
        this.pullTimeout = PullSubjectsConfig.get().getPullTimeout(subject);
        this.ackNosendLimit = PullSubjectsConfig.get().getAckNosendLimit(subject);

        String[] values = new String[]{subject, consumerGroup};
        this.pullRunCounter = Metrics.counter("qmq_pull_run_count", SUBJECT_GROUP_ARRAY, values);
        this.pauseCounter = Metrics.counter("qmq_pull_pause_count", SUBJECT_GROUP_ARRAY, values);

        subscribeOnlineStateChanged(consumerOnlineStateManager, subject, consumerGroup);
    }

    @Override
    public void startPull(ExecutorService executor) {
        executor.submit(this);
    }

    private static final int PREPARE_PULL = 0;
    private static final int PULL_DONE = 1;

    private final AtomicInteger state = new AtomicInteger();
    private volatile PullParam pullParam;
    private volatile PullService.PullResultFuture pullFuture;

    @Override
    public void run() {
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

                    final DoPullParam doPullParam = new DoPullParam();
                    if (await(resetDoPullParam(doPullParam))) {
                        return;
                    }

                    AckSendInfo ackSendInfo = ackSendQueue.getAckSendInfo();
                    pullParam = buildPullParam(consumeParam, doPullParam.brokerGroup, ackSendInfo, pullBatchSize.get(),
                            pullTimeout.get());
                    pullFuture = pullService.pullAsync(pullParam);
                    state.set(PULL_DONE);
                    pullFuture.addListener(this, partitionExecutor);
                    break;
                case PULL_DONE:
                    final PullParam thisPullParam = pullParam;
                    final BrokerGroupInfo brokerGroup = thisPullParam.getBrokerGroup();
                    try {
                        PullResult pullResult = pullFuture.get();
                        List<PulledMessage> messages = handlePullResult(thisPullParam, pullResult,
                                consumeMessageExecutor.getMessageHandler());
                        brokerGroup.markSuccess();
                        pullStrategy.record(messages.size() > 0);
                        consumeMessageExecutor.consume(messages);
                    } catch (ExecutionException e) {
                        markFailed(brokerGroup);
                        Throwable cause = e.getCause();
                        //超时异常暂时不打印日志了
                        if (!(cause instanceof TimeoutException)) {
                            LOGGER.error("pull message exception. {}", thisPullParam, e);
                        }
                    } catch (Exception e) {
                        markFailed(brokerGroup);
                        LOGGER.error("pull message exception. {}", thisPullParam, e);
                    } finally {
                        state.set(PREPARE_PULL);
                        run();
                    }
                    break;
            }
        } catch (Throwable t) {
            LOGGER.error("pull error subject {} consumerGroup {}", getSubject(), getConsumerGroup(), t);
        }
    }

    private boolean await(ListenableFuture future) {
        if (future == null) {
            return false;
        }
        future.addListener(this, partitionExecutor);
        return true;
    }

    private ListenableFuture waitOnline() {
        synchronized (this.isOnline) {
            if (isOnline.get()) {
                return null;
            }
            SettableFuture waitOnlineFuture = SettableFuture.create();
            this.waitOnlineFuture = waitOnlineFuture;
            return waitOnlineFuture;
        }
    }

    private ListenableFuture preparePull() {
        pullRunCounter.inc();
        if (consumeMessageExecutor.isFull()) {
            return delay("wait consumer", PAUSETIME_OF_CLEAN_LAST_MESSAGE);
        }

        if (!pullStrategy.needPull()) {
            return delay("wait consumer", PAUSETIME_OF_NOMESSAGE);
        }
        return null;
    }

    private ListenableFuture resetDoPullParam(DoPullParam param) {

        if (ackSendQueue.getAckSendInfo().getToSendNum() > ackNosendLimit.get()) {
            return delay("wait ack", PAUSETIME_OF_NOAVAILABLE_BROKER);
        }

        BrokerClusterInfo brokerCluster = getBrokerCluster();
        BrokerGroupInfo brokerGroup = brokerCluster.getGroupByName(getBrokerGroup());
        if (BrokerGroupInfo.isInvalid(brokerGroup)) {
            return delay("no available broker", PAUSETIME_OF_NOAVAILABLE_BROKER);
        }

        param.ackSendInfo = ackSendQueue.getAckSendInfo();
        param.brokerGroup = brokerGroup;
        return null;
    }

    private BrokerClusterInfo getBrokerCluster() {
        return brokerService.getConsumerBrokerCluster(ClientType.CONSUMER, consumeParam.getSubject());
    }

    private ListenableFuture delay(String log, long timeMillis) {
        final String subject = consumeParam.getSubject();
        final String consumerGroup = consumeParam.getConsumerGroup();
        this.pauseCounter.inc();
        LOGGER.debug("pull pause {} ms, {}. subject={}, consumerGroup={}", timeMillis, log, subject, consumerGroup);
        RunnableSettableFuture future = new RunnableSettableFuture();
        DELAY_SCHEDULER.schedule(future, timeMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    private static class RunnableSettableFuture extends AbstractFuture implements Runnable {

        @Override
        public void run() {
            super.set(null);
        }
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

    private void subscribeOnlineStateChanged(ConsumerOnlineStateManager consumerOnlineStateManager, String subject,
            String consumerGroup) {
        SwitchWaiter switchWaiter = consumerOnlineStateManager.getSwitchWaiter(subject, consumerGroup);
        isOnline.set(switchWaiter.isOnline());
        switchWaiter.addListener(isOnline -> {
            synchronized (this.isOnline) {
                this.isOnline.set(isOnline);
                final SettableFuture future = waitOnlineFuture;
                if (future != null && isOnline) {
                    future.set(null);
                }
            }
        });
    }

    private static final class DoPullParam {

        private volatile AckSendInfo ackSendInfo = null;
        private volatile BrokerGroupInfo brokerGroup = null;
    }
}