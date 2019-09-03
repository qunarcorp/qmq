package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.AbstractMessageExecutor;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class OrderedMessageExecutor extends AbstractMessageExecutor {

    private final static int MAX_QUEUE_SIZE = 10000;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Logger logger = LoggerFactory.getLogger(OrderedMessageExecutor.class);

    private BlockingQueue<PulledMessage> messageQueue;
    private MessageHandler messageHandler;
    private Executor executor;

    public OrderedMessageExecutor(String subject, String group, Executor executor, MessageListener messageListener) {
        super(subject, group);
        this.executor = executor;
        this.messageHandler = new BaseMessageHandler(messageListener);
        this.messageQueue = new LinkedBlockingQueue<>();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            executor.execute(this::processMessages);
        }
    }

    private void processMessages() {
        while (true) {
            try {
                PulledMessage message = messageQueue.take();
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
}
