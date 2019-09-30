package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class CompositePullConsumer<T extends PullConsumer> extends AbstractCompositePullClient<T> implements PullConsumer, CompositePullClient<T> {

    public CompositePullConsumer(
            String subject,
            String consumerGroup,
            String consumerId,
            int version,
            boolean isBroadcast,
            boolean isOrdered,
            long consumptionExpiredTime,
            List<T> consumers,
            ConsumerOnlineStateManager consumerOnlineStateManager) {
        super(subject, consumerGroup, "", "", consumerId, null, version, isBroadcast, isOrdered, consumptionExpiredTime, consumers);
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
        List<Message> result = Lists.newArrayListWithCapacity(size);
        for (PullConsumer consumer : getComponents()) {
            result.addAll(consumer.pull(size));
            if (result.size() >= size) {
                break;
            }
        }
        return result;
    }

    @Override
    public List<Message> pull(int size, long timeoutMillis) {
        List<Message> result = Lists.newArrayListWithCapacity(size);
        for (PullConsumer consumer : getComponents()) {
            long start = System.currentTimeMillis();
            result.addAll(consumer.pull(size, timeoutMillis));
            if (result.size() >= size) {
                break;
            }
            long elapsed = System.currentTimeMillis() - start;
            timeoutMillis = timeoutMillis - elapsed;
            if (elapsed <= 0) {
                break;
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
            ListenableFuture<List<Message>> future = (ListenableFuture<List<Message>>) consumer.pullFuture(size, timeoutMillis, isResetCreateTime);
            return chainNextPullFuture(size, timeoutMillis, isResetCreateTime, System.currentTimeMillis(), future, new AtomicInteger(0));
        }
    }

    private ListenableFuture<List<Message>> chainNextPullFuture(int size, long timeoutMillis, boolean isResetCreateTime, long startMills, ListenableFuture<List<Message>> future, AtomicInteger currentConsumerIdx) {
        return Futures.transform(future, new AsyncFunction<List<Message>, List<Message>>() {
            @Override
            public ListenableFuture<List<Message>> apply(List<Message> messages) throws Exception {
                int messageLeft = size - messages.size();
                long elapsed = System.currentTimeMillis() - startMills;
                long timeoutLeft = timeoutMillis - elapsed;
                if (currentConsumerIdx.get() == (getComponents().size() - 1) || messageLeft <= 0 || timeoutLeft <= 0) {
                    // 最后一个 Consumer / 消息条数够了 / 超时, 则直接返回
                    return future;
                }
                PullConsumer nextConsumer = getComponents().get(currentConsumerIdx.incrementAndGet());
                // 还不够
                ListenableFuture<List<Message>> nextFuture = (ListenableFuture<List<Message>>) nextConsumer.pullFuture(messageLeft, timeoutLeft, isResetCreateTime);
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
                return chainNextPullFuture(messageLeft, timeoutLeft, isResetCreateTime, System.currentTimeMillis(), nextFuture, currentConsumerIdx);
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
}
