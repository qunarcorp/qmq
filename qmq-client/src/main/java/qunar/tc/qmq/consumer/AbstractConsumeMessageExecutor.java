package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public abstract class AbstractConsumeMessageExecutor implements ConsumeMessageExecutor {

    private static final int MAX_QUEUE_SIZE = 10000;

    private static final Logger logger = LoggerFactory.getLogger(AbstractConsumeMessageExecutor.class);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;
    private final BlockingDeque<PulledMessage> messageQueue;
    private final Executor partitionExecutor;
    private final String subject;
    private final String consumerGroup;
    private final String partitionName;
    private final MessageHandler messageHandler;

    private volatile long consumptionExpiredTime;

    public AbstractConsumeMessageExecutor(String subject, String consumerGroup, String partitionName, Executor partitionExecutor, MessageHandler messageHandler, long consumptionExpiredTime) {
        this.consumptionExpiredTime = consumptionExpiredTime;
        String[] values = {subject, consumerGroup};
        this.createToHandleTimer = Metrics.timer("qmq_pull_createToHandle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleTimer = Metrics.timer("qmq_pull_handle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleFailCounter = Metrics.counter("qmq_pull_handleFail_count", SUBJECT_GROUP_ARRAY, values);
        this.messageQueue = new LinkedBlockingDeque<>();
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.partitionName = partitionName;
        this.partitionExecutor = partitionExecutor;
        this.messageHandler = messageHandler;
    }

    @Override
    public void setConsumptionExpiredTime(long timestamp) {
        this.consumptionExpiredTime = timestamp;
    }

    @Override
    public long getConsumptionExpiredTime() {
        return consumptionExpiredTime;
    }

    public QmqTimer getCreateToHandleTimer() {
        return createToHandleTimer;
    }

    public QmqTimer getHandleTimer() {
        return handleTimer;
    }

    public QmqCounter getHandleFailCounter() {
        return handleFailCounter;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            partitionExecutor.execute(this::processMessages);
        }
    }

    private void processMessages() {
        while (started.get()) {
            PulledMessage message;
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                logger.error("获取 queue 消息被中断 subject {} group {} {}", subject, consumerGroup, e.getMessage());
                continue;
            }
            try {
                processMessage(message);
            } catch (Throwable t) {
                logger.error("消息处理失败 ", t);
            }
        }
    }

    abstract void processMessage(PulledMessage message);

    @Override
    public boolean consume(List<PulledMessage> messages) {
        for (PulledMessage message : messages) {
            messageQueue.offer(message);
        }
        return true;
    }

    @Override
    public boolean isFull() {
        // 如果堆积的消息数超过 maxQueueSize, 则不会再拉取消息
        return messageQueue.size() < MAX_QUEUE_SIZE;
    }

    @Override
    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public boolean requeueFirst(PulledMessage message) {
        try {
            messageQueue.putFirst(message);
            return true;
        } catch (InterruptedException e) {
            logger.error("消息重入 queue 被中断 subject {} group {} id {}", subject, consumerGroup, message.getMessageId(), e);
            return false;
        }
    }

    @Override
    public void destroy() {
        started.compareAndSet(true, false);
    }
}
