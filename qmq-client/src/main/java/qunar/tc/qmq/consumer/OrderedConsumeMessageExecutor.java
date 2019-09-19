package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.ClientMetaManager;
import qunar.tc.qmq.common.ExclusiveConsumerLifecycleManager;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.common.StrictOrderStrategy;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.meta.ConsumerAllocation;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class OrderedConsumeMessageExecutor extends AbstractConsumeMessageExecutor {
    private final Logger logger = LoggerFactory.getLogger(OrderedConsumeMessageExecutor.class);

    private final static int MAX_QUEUE_SIZE = 10000;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final ExclusiveConsumerLifecycleManager lifecycleManager;
    private final BlockingDeque<PulledMessage> messageQueue;
    private final MessageHandler messageHandler;
    private final Executor partitionExecutor;
    private final Executor messageHandleExecutor;
    private final ClientMetaManager clientMetaManager;
    private final String subject;
    private final String consumerGroup;
    private final String partitionName;
    private final ConsumeStrategy consumeStrategy;
    private volatile boolean stopped = false;

    public OrderedConsumeMessageExecutor(
            String subject,
            String consumerGroup,
            String partitionName,
            ConsumeStrategy consumeStrategy,
            Executor partitionExecutor,
            Executor messageHandleExecutor,
            MessageListener messageListener,
            ExclusiveConsumerLifecycleManager lifecycleManager,
            ClientMetaManager clientMetaManager
    ) {
        super(subject, consumerGroup);
        this.partitionName = partitionName;
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.consumeStrategy = consumeStrategy;
        this.partitionExecutor = partitionExecutor;
        this.messageHandleExecutor = messageHandleExecutor;
        this.lifecycleManager = lifecycleManager;
        this.messageHandler = new BaseMessageHandler(messageListener);
        this.messageQueue = new LinkedBlockingDeque<>();
        this.clientMetaManager = clientMetaManager;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            partitionExecutor.execute(this::processMessages);
        }
    }

    private void processMessages() {
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(subject);
        while (!stopped) {
            PulledMessage message;
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                logger.error("获取 queue 消息被中断 subject {} group {} {}", subject, consumerGroup, e.getMessage());
                continue;
            }
            try {
                String brokerGroup = message.getStringProperty(BaseMessage.keys.qmq_partitionBroker);
                if (!lifecycleManager.isAlive(subject, consumerGroup, brokerGroup, partitionName)) {
                    // 没有权限, 停一会再看
                    Thread.sleep(10);
                    messageQueue.putFirst(message);
                    continue;
                }

                MessageExecutionTask task = new MessageExecutionTask(message, messageHandler, getCreateToHandleTimer(), getHandleTimer(), getHandleFailCounter());
                if (Objects.equals(ConsumeStrategy.EXCLUSIVE, consumeStrategy)) {
                    // 独占消费使用单线程逐个任务处理
                    boolean success = task.run();
                    if (!success) {
                        // 消息消费不成功, 根据顺序策略处理
                        orderStrategy.onConsumeFailed(message, this);
                    } else if (!task.isAcked() && Objects.equals(orderStrategy.name(), StrictOrderStrategy.NAME)) {
                        // 严格有序消息如果没有 ACK 当做消费失败处理
                        orderStrategy.onConsumeFailed(message, this);
                    }
                } else {
                    // 共享消费
                    task.run(messageHandleExecutor);
                }
            } catch (Throwable t) {
                orderStrategy.onConsumeFailed(message, this);
                logger.error("消息处理失败 ", t);
            }
        }
    }

    @Override
    public boolean consume(List<PulledMessage> messages) {
        for (PulledMessage message : messages) {
            messageQueue.offer(message);
        }
        return true;
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
    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    public boolean isFull() {
        // 如果堆积的消息数超过 maxQueueSize, 则不会再拉取消息
        return messageQueue.size() < MAX_QUEUE_SIZE;
    }

    @Override
    public void destroy() {
        stopped = true;
    }
}
