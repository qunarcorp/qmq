package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class CompositePullConsumer<T extends PullConsumer> extends AbstractCompositePullClient<T> implements
        PullConsumer, CompositePullClient<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositePullConsumer.class);

    private static final long SLEEP_ROUND_MS = 10;
    private static final long MAX_SLEEP_ROUND_MS = 100;

    public CompositePullConsumer(
            String subject,
            String consumerGroup,
            String consumerId,
            ConsumeStrategy consumeStrategy,
            int allocationVersion,
            boolean isBroadcast,
            boolean isOrdered,
            int partitionSetVersion,
            long consumptionExpiredTime,
            List<T> consumers,
            ConsumerOnlineStateManager consumerOnlineStateManager) {
        super(subject, consumerGroup, "", consumerId, consumeStrategy, allocationVersion, isBroadcast, isOrdered,
                partitionSetVersion, consumptionExpiredTime, consumers);
        SwitchWaiter switchWaiter = consumerOnlineStateManager.getSwitchWaiter(subject, consumerGroup);
        switchWaiter.on(StatusSource.HEALTHCHECKER);
    }

    @Override
    public void setConsumeMostOnce(boolean consumeMostOnce) {
        getComponents().forEach(pc -> pc.setConsumeMostOnce(consumeMostOnce));
    }

    @Override
    public boolean isConsumeMostOnce() {
        List<T> components = getComponents();
        if (CollectionUtils.isEmpty(components)) {
            return false;
        } else {
            return components.get(0).isConsumeMostOnce();
        }
    }

    @Override
    public List<Message> pull(int size) {
        return pull(size, MAX_PULL_TIMEOUT_MILLIS);
    }

    @Override
    public List<Message> pull(int size, long timeoutMillis) {
        return pull(size, timeoutMillis, true);
    }

    @Override
    public List<Message> pull(int size, long timeoutMillis, boolean waitIfNotComplete) {
        long sleepMs = SLEEP_ROUND_MS;
        List<Message> result = Lists.newArrayListWithCapacity(size);
        int left = size;
        while (result.size() < size) { // 无限拉, 直到超时或拉满
            boolean emptyRound = true;
            for (PullConsumer consumer : getComponents()) {
                long start = System.currentTimeMillis();
                // 这里超时时间必须设为 -1, 否则 server 端会被挂起
                List<Message> pullResult = consumer.pull(left, -1, false);
                if (!CollectionUtils.isEmpty(pullResult)) {
                    result.addAll(pullResult);
                    left -= pullResult.size();
                    emptyRound = false;
                }
                if (result.size() >= size) {
                    // 拉满
                    return result;
                }

                if (timeoutMillis > 0) { // <0 为没有超时时间
                    long elapsed = System.currentTimeMillis() - start;
                    long timeLeft = timeoutMillis - elapsed;
                    if (timeLeft <= 0) {
                        // 超时
                        return result;
                    }
                }
            }

            if (timeoutMillis < 0) {
                // 没有超时时间且全部遍历完则直接返回
                return result;
            }

            if (emptyRound) {
                // 拉完如果没有消息则 sleep 时间翻倍
                sleepMs = Math.min(sleepMs * 2, MAX_SLEEP_ROUND_MS);
            } else {
                sleepMs = SLEEP_ROUND_MS;
            }

            if (!waitIfNotComplete) {
                return result;
            }

            try {
                Thread.sleep(sleepMs);
                timeoutMillis -= sleepMs;
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return result;
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size) {
        return pullFuture(size, MAX_PULL_TIMEOUT_MILLIS, DEFAULT_RESET_CREATE_TIME);
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size, long timeoutMillis) {
        return pullFuture(size, timeoutMillis, DEFAULT_RESET_CREATE_TIME);
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size, long timeoutMillis, boolean isResetCreateTime) {
        List<T> components = getComponents();
        if (CollectionUtils.isEmpty(components)) {
            return Futures.immediateFuture(Collections.emptyList());
        } else {
            PullConsumer consumer = components.get(0);
            ListenableFuture<List<Message>> future = (ListenableFuture<List<Message>>) consumer
                    .pullFuture(size, -1, isResetCreateTime);
            return chainNextPullFuture(size, timeoutMillis, isResetCreateTime, System.currentTimeMillis(), future,
                    new AtomicInteger(0), SLEEP_ROUND_MS);
        }
    }

    private ListenableFuture<List<Message>> chainNextPullFuture(int size, long timeoutMillis, boolean isResetCreateTime,
            long startMills, ListenableFuture<List<Message>> future, AtomicInteger currentConsumerIdx, final long sleepRoundMs) {
        return Futures.transform(future, new AsyncFunction<List<Message>, List<Message>>() {
            @Override
            public ListenableFuture<List<Message>> apply(List<Message> messages) throws Exception {
                int messageLeft = size - messages.size();
                long elapsed = System.currentTimeMillis() - startMills;
                long timeoutLeft = timeoutMillis < 0 ? timeoutMillis : timeoutMillis - elapsed;
                if (messageLeft <= 0 || (timeoutMillis > 0 && timeoutLeft <= 0)) {
                    // timeout < 0 代表没有超时时间
                    // 消息条数够了 / 超时, 则直接返回
                     return future;
                }
                List<T> components = getComponents();
                int nextIndex = currentConsumerIdx.incrementAndGet() % components.size();
                long sleepTime = sleepRoundMs;

                if (nextIndex == 0) {
                    // 说明已经查过一轮
                    if (timeoutMillis < 0) {
                        return future;
                    } else {
                        // 继续循环
                        if (!CollectionUtils.isEmpty(messages)) {
                            sleepTime = Math.min(sleepRoundMs * 2, MAX_SLEEP_ROUND_MS);
                        } else {
                            sleepTime = SLEEP_ROUND_MS;
                        }
                        Thread.sleep(sleepTime);
                        timeoutLeft -= sleepTime;
                    }
                }

                PullConsumer nextConsumer = components.get(nextIndex);
                // 还不够
                ListenableFuture<List<Message>> nextFuture = (ListenableFuture<List<Message>>) nextConsumer
                        .pullFuture(messageLeft, -1, isResetCreateTime);
                nextFuture = Futures.transform(nextFuture, (Function<List<Message>, List<Message>>) nextResult -> {
                    // 合并下一个 future 和当前 future 的消息
                    if (nextResult == null) {
                        nextResult = Collections.emptyList();
                    }
                    ArrayList<Message> result = Lists.newArrayListWithCapacity(nextResult.size() + messages.size());
                    result.addAll(nextResult);
                    result.addAll(messages);
                    return result;
                });
                return chainNextPullFuture(messageLeft, timeoutLeft, isResetCreateTime, System.currentTimeMillis(),
                        nextFuture, currentConsumerIdx, sleepTime);
            }
        });
    }

    @Override
    public String getClientId() {
        return getConsumerId();
    }

    @Override
    public void close() throws Exception {
        for (PullConsumer consumer : getComponents()) {
            consumer.close();
        }
    }

    @Override
    public void online() {
        getComponents().forEach(PullConsumer::online);
    }

    @Override
    public void offline() {
        getComponents().forEach(PullConsumer::offline);
    }
}
