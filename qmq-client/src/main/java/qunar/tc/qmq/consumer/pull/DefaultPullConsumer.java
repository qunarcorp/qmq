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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.metrics.Metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-9-20.
 */
class DefaultPullConsumer extends AbstractPullConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullConsumer.class);

    private static final int POLL_TIMEOUT_MILLIS = 1000;
    private static final int MAX_FETCH_SIZE = 10000;
    private static final int MAX_TIMEOUT = 5 * 60 * 1000;

    private volatile boolean isStop = false;

    private final LinkedBlockingQueue<PullMessageFuture> requestQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Message> localBuffer = new LinkedBlockingQueue<>();

    private final int preFetchSize;
    private final int lowWaterMark;

    DefaultPullConsumer(
            String subject,
            String consumerGroup,
            String partitionName,
            String brokerGroup,
            ConsumeStrategy consumeStrategy,
            int version,
            long consumptionExpiredTime,
            boolean isBroadcast,
            boolean isOrdered,
            String clientId,
            PullService pullService,
            AckService ackService,
            BrokerService brokerService,
            SendMessageBack sendMessageBack) {
        super(subject, consumerGroup, partitionName, brokerGroup, consumeStrategy, version, consumptionExpiredTime, isBroadcast, isOrdered, clientId, pullService, ackService, brokerService, sendMessageBack);
        this.preFetchSize = PullSubjectsConfig.get().getPullBatchSize(subject).get();
        this.lowWaterMark = Math.round(preFetchSize * 0.2F);
    }

    @Override
    PullMessageFuture newFuture(int size, long timeout, boolean isResetCreateTime) {
        if (size > MAX_FETCH_SIZE) {
            size = MAX_FETCH_SIZE;
        }
        if (timeout > MAX_TIMEOUT) {
            timeout = MAX_TIMEOUT;
        }

        if (size <= 0) {
            PullMessageFuture future = new PullMessageFuture(size, size, timeout, isResetCreateTime);
            future.set(Collections.<Message>emptyList());
            return future;
        }

        if (localBuffer.size() == 0) {
            int fetchSize = Math.max(size, preFetchSize);
            PullMessageFuture future = new PullMessageFuture(size, fetchSize, timeout, isResetCreateTime);
            requestQueue.offer(future);
            return future;
        }

        if (localBuffer.size() >= size) {
            List<Message> messages = new ArrayList<>(size);
            localBuffer.drainTo(messages, size);
            if (messages.size() > 0) {
                checkLowWaterMark();
                PullMessageFuture future = new PullMessageFuture(size, size, timeout, isResetCreateTime);
                future.set(messages);
                return future;
            }
        }
        int fetchSize = Math.max(preFetchSize, size - localBuffer.size());
        PullMessageFuture future = new PullMessageFuture(size, fetchSize, timeout, isResetCreateTime);
        requestQueue.offer(future);
        return future;
    }

    private void checkLowWaterMark() {
        int bufferSize = localBuffer.size();
        if (bufferSize < lowWaterMark) {
            int fetchSize = preFetchSize - bufferSize;
            PullMessageFuture future = new PullMessageFuture(0, fetchSize, -1, false);
            requestQueue.offer(future);
        }
    }

    private void doPull(PullMessageFuture request) {
        List<Message> messages = Lists.newArrayListWithCapacity(request.getFetchSize());
        try {
            retryPullEntry.pull(request.getFetchSize(), request.getTimeout(), messages);
            if (messages.size() > 0 && request.isPullOnce()) return;

            if (request.isResetCreateTime()) {
                request.resetCreateTime();
            }

            do {
                int fetchSize = request.getFetchSize() - messages.size();
                if (fetchSize <= 0) break;
                PlainPullEntry.PlainPullResult result = pullEntry.pull(fetchSize, request.getTimeout(), messages);
                if (result == PlainPullEntry.PlainPullResult.NO_BROKER) {
                    break;
                }
            } while (messages.size() < request.getFetchSize() && !request.isExpired());
        } catch (Exception e) {
            LOGGER.error("DefaultPullConsumer doPull exception. subject={}, group={}", subject(), group(), e);
            Metrics.counter("qmq_pull_defaultPull_doPull_fail", SUBJECT_GROUP_ARRAY, new String[]{subject(), group()}).inc();
        } finally {
            setResult(request, messages);
        }
    }

    private void setResult(PullMessageFuture future, List<Message> messages) {
        int expectedSize = future.getExpectedSize();
        if (expectedSize <= 0) {
            localBuffer.addAll(messages);
            future.set(Collections.<Message>emptyList());
            return;
        }

        List<Message> result = new ArrayList<>(expectedSize);
        int bufferSize = localBuffer.size();
        if (bufferSize > 0) {
            localBuffer.drainTo(result, Math.min(expectedSize, bufferSize));
        }
        int need = expectedSize - result.size();
        if (need <= 0) {
            localBuffer.addAll(messages);
            future.set(result);
            return;
        }

        result.addAll(head(messages, need));
        localBuffer.addAll(tail(messages, need));
        future.set(result);
    }

    private List<Message> head(List<Message> input, int head) {
        if (head >= input.size()) {
            return input;
        }
        return input.subList(0, head);
    }

    private List<Message> tail(List<Message> input, int head) {
        if (head >= input.size()) {
            return Collections.emptyList();
        }
        return input.subList(head, input.size());
    }

    @Override
    public void close() {
        isStop = true;
    }

    @Override
    public void startPull(ExecutorService executor) {
        executor.submit(() -> {

            PullMessageFuture future;
            while (!isStop) {
                try {
                    if (!onlineSwitcher.waitOn()) continue;

                    future = requestQueue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    if (future != null) doPull(future);
                } catch (InterruptedException e) {
                    LOGGER.error("pullConsumer poll be interrupted. subject={}, group={}", subject(), group(), e);
                } catch (Exception e) {
                    LOGGER.error("pullConsumer poll exception. subject={}, group={}", subject(), group(), e);
                }
            }
        });
    }

    @Override
    public void stopPull() {

    }

    @Override
    public void destroy() {
        close();
        super.destroy();
    }
}
