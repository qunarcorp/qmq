package qunar.tc.qmq.consumer.pull;

import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.PartitionPullClient;
import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.StatusSource;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class PartitionPullConsumer implements PullConsumer, PartitionPullClient {

    private int partition;
    private PullConsumer pullConsumer;

    public PartitionPullConsumer(int partition, PullConsumer pullConsumer) {
        this.partition = partition;
        this.pullConsumer = pullConsumer;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public void startPull(ExecutorService executor) {
        pullConsumer.startPull(executor);
    }

    @Override
    public void destroy() {
        pullConsumer.destroy();
    }

    @Override
    public void online(StatusSource statusSource) {
        pullConsumer.online(statusSource);
    }

    @Override
    public void offline(StatusSource statusSource) {
        pullConsumer.offline(statusSource);
    }

    @Override
    public void online() {
        pullConsumer.online();
    }

    @Override
    public void offline() {
        pullConsumer.offline();
    }

    @Override
    public void setConsumeMostOnce(boolean consumeMostOnce) {
        pullConsumer.setConsumeMostOnce(consumeMostOnce);
    }

    @Override
    public boolean isConsumeMostOnce() {
        return pullConsumer.isConsumeMostOnce();
    }

    @Override
    public List<Message> pull(int size) {
        return pullConsumer.pull(size);
    }

    @Override
    public List<Message> pull(int size, long timeoutMillis) {
        return pullConsumer.pull(size, timeoutMillis);
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size) {
        return (ListenableFuture<List<Message>>) pullConsumer.pullFuture(size);
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size, long timeoutMillis) {
        return (ListenableFuture<List<Message>>) pullConsumer.pullFuture(size, timeoutMillis);
    }

    @Override
    public ListenableFuture<List<Message>> pullFuture(int size, long timeoutMillis, boolean isResetCreateTime) {
        return (ListenableFuture<List<Message>>) pullConsumer.pullFuture(size, timeoutMillis, isResetCreateTime);
    }

    @Override
    public String getClientId() {
        return pullConsumer.getClientId();
    }

    @Override
    public String subject() {
        return pullConsumer.subject();
    }

    @Override
    public String group() {
        return pullConsumer.group();
    }

    @Override
    public void close() throws Exception {
        pullConsumer.close();
    }
}
