package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.*;
import qunar.tc.qmq.broker.BrokerService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class CompositePullConsumer<T extends PullConsumer> extends AbstractPullClient implements PullConsumer, CompositePullClient<T> {

    private List<T> consumers;

    public CompositePullConsumer(String subject, String consumerGroup, int version, long consumptionExpiredTime, List<T> consumers, BrokerService brokerService) {
        super(subject, consumerGroup, "", "", null, version, consumptionExpiredTime, brokerService);
        this.consumers = consumers;
    }

    @Override
    public void online() {
        consumers.forEach(PullConsumer::online);
    }

    @Override
    public void offline() {
        consumers.forEach(PullConsumer::offline);
    }

    @Override
    public void setConsumeMostOnce(boolean consumeMostOnce) {
        consumers.forEach(pc -> pc.setConsumeMostOnce(consumeMostOnce));
    }

    @Override
    public boolean isConsumeMostOnce() {
        return consumers.get(0).isConsumeMostOnce();
    }

    @Override
    public List<Message> pull(int size) {
        List<Message> result = Lists.newArrayListWithCapacity(size);
        for (PullConsumer consumer : consumers) {
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
        for (PullConsumer consumer : consumers) {
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
        PullConsumer consumer = consumers.get(0);
        ListenableFuture<List<Message>> future = (ListenableFuture<List<Message>>) consumer.pullFuture(size, timeoutMillis, isResetCreateTime);
        return chainNextPullFuture(size, timeoutMillis, isResetCreateTime, System.currentTimeMillis(), future, new AtomicInteger(0));
    }

    private ListenableFuture<List<Message>> chainNextPullFuture(int size, long timeoutMillis, boolean isResetCreateTime, long startMills, ListenableFuture<List<Message>> future, AtomicInteger currentConsumerIdx) {
        return Futures.transform(future, new AsyncFunction<List<Message>, List<Message>>() {
            @Override
            public ListenableFuture<List<Message>> apply(List<Message> messages) throws Exception {
                int messageLeft = size - messages.size();
                long elapsed = System.currentTimeMillis() - startMills;
                long timeoutLeft = timeoutMillis - elapsed;
                if (currentConsumerIdx.get() == (consumers.size() - 1) || messageLeft <= 0 || timeoutLeft <= 0) {
                    // 最后一个 Consumer / 消息条数够了 / 超时, 则直接返回
                    return future;
                }
                PullConsumer nextConsumer = consumers.get(currentConsumerIdx.incrementAndGet());
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
        return consumers.get(0).getClientId();
    }

    @Override
    public void close() throws Exception {
        for (PullConsumer consumer : consumers) {
            consumer.close();
        }
    }

    @Override
    public String subject() {
        return consumers.get(0).subject();
    }

    @Override
    public String group() {
        return consumers.get(0).group();
    }

    @Override
    public void startPull(ExecutorService executor) {
        consumers.forEach(pc -> pc.startPull(executor));
    }

    @Override
    public void stopPull() {
        consumers.forEach(PullClient::stopPull);
    }

    @Override
    public void destroy() {
        consumers.forEach(PullConsumer::destroy);
    }

    @Override
    public void online(StatusSource statusSource) {
        consumers.forEach(pc -> pc.online(statusSource));
    }

    @Override
    public void offline(StatusSource statusSource) {
        consumers.forEach(pc -> pc.offline(statusSource));
    }

    @Override
    public List<T> getComponents() {
        return consumers;
    }
}
