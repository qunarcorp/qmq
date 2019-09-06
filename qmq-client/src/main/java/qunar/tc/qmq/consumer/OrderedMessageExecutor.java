package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.AbstractMessageExecutor;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.OrderStrategyManager;
import qunar.tc.qmq.broker.OrderStrategy;
import qunar.tc.qmq.common.OrderedClientLifecycleManager;
import qunar.tc.qmq.consumer.pull.PulledMessage;

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
public class OrderedMessageExecutor extends AbstractMessageExecutor {

    private final static int MAX_QUEUE_SIZE = 10000;
    private final static int MAX_RETRY = 3;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Logger logger = LoggerFactory.getLogger(OrderedMessageExecutor.class);

    private OrderedClientLifecycleManager lifecycleManager;
    private BlockingDeque<PulledMessage> messageQueue;
    private MessageHandler messageHandler;
    private Executor executor;
    private String subject;
    private String group;
    private volatile boolean stopped = false;

    public OrderedMessageExecutor(String subject, String group, Executor executor, MessageListener messageListener, OrderedClientLifecycleManager lifecycleManager) {
        super(subject, group);
        this.subject = subject;
        this.group = group;
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
        int retry = 0;
        while (!stopped) {
            PulledMessage message;
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                logger.error("获取 queue 消息被中断 subject {} group {}", subject, group, e.getMessage());
                continue;
            }
            try {
                int partition = message.getIntProperty(BaseMessage.keys.qmq_physicalPartition.name());
                if (!lifecycleManager.isAlive(subject, group, partition)) {
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
                if (Objects.equals(OrderStrategy.STRICT, orderStrategy) && retry < MAX_RETRY) {
                    retry++;
                    try {
                        messageQueue.putFirst(message);
                    } catch (InterruptedException e) {
                        logger.error("消息重入 queue 被中断 subject {} group {} id {}", subject, group, message.getMessageId(), e);
                    }
                }
                logger.error("消息处理失败 ", t);
            }
        }
    }

    @Override
    public boolean execute(List<PulledMessage> messages) {
        for (PulledMessage message : messages) {
            messageQueue.offer(message);
        }
        return true;
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
