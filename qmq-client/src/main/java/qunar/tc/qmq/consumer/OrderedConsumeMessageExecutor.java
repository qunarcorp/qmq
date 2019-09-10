package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.AbstractConsumeMessageExecutor;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.ExclusiveConsumerLifecycleManager;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.sender.OrderStrategy;
import qunar.tc.qmq.producer.sender.OrderStrategyManager;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class OrderedConsumeMessageExecutor extends AbstractConsumeMessageExecutor {

    private final static int MAX_QUEUE_SIZE = 10000;
    private final static int MAX_RETRY = 3;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Logger logger = LoggerFactory.getLogger(OrderedConsumeMessageExecutor.class);

    private ExclusiveConsumerLifecycleManager lifecycleManager;
    private BlockingDeque<PulledMessage> messageQueue;
    private MessageHandler messageHandler;
    private Executor executor;
    private String subject;
    private String consumerGroup;
    private volatile boolean stopped = false;

    public OrderedConsumeMessageExecutor(String subject, String consumerGroup, Executor executor, MessageListener messageListener, ExclusiveConsumerLifecycleManager lifecycleManager) {
        super(subject, consumerGroup);
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.executor = executor;
        this.lifecycleManager = lifecycleManager;
        this.messageHandler = new BaseMessageHandler(messageListener);
        this.messageQueue = new LinkedBlockingDeque<>();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            executor.execute(this::processMessages);
        }
    }

    private void processMessages() {
        OrderStrategy orderStrategy = OrderStrategyManager.getOrderStrategy(subject);
        while (!stopped) {
            PulledMessage message;
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                logger.error("获取 queue 消息被中断 subject {} group {} {}", subject, consumerGroup, e.getMessage());
                continue;
            }
            try {
                String partitionName = message.getStringProperty(BaseMessage.keys.qmq_partitionName);
                String brokerGroup = message.getStringProperty(BaseMessage.keys.qmq_partitionBroker);
                if (!lifecycleManager.isAlive(subject, consumerGroup, brokerGroup, partitionName)) {
                    // 没有权限, 停一会再看
                    Thread.sleep(10);
                    messageQueue.putFirst(message);
                    continue;
                }

                boolean originalAutoAck = message.isAutoAck();
                MessageExecutionTask task = new MessageExecutionTask(message, null, messageHandler, getCreateToHandleTimer(), getHandleTimer(), getHandleFailCounter());
                if (originalAutoAck) {
                    // 禁用 task 内部的 ack
                    message.autoAck(false);
                }
                task.process();
                if (originalAutoAck) {
                    // 如果用户手动关闭 autoAck, 则把 ack 的任务交给用户
                    message.ack(null);
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

    @Override
    public boolean cleanUp() {
        // 如果堆积的消息数超过 maxQueueSize, 则不会再拉取消息
        return messageQueue.size() < MAX_QUEUE_SIZE;
    }

    @Override
    public void destroy() {
        stopped = true;
    }
}
