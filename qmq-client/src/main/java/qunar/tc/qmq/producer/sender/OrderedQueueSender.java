package qunar.tc.qmq.producer.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.producer.ConfigCenter;
import qunar.tc.qmq.producer.QueueSender;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 消息发送队列管理器, 每个队列由一个 MessageGroup 代表
 *
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedQueueSender implements QueueSender {

    private static final Logger logger = LoggerFactory.getLogger(OrderedQueueSender.class);

    private final ExecutorService executor; // 所有分区共享一个线程池
    private final LinkedBlockingDeque<ProduceMessage> queue;
    private final SendMessageExecutorManager sendMessageExecutorManager;
    private final MessageSender messageSender;
    private final int maxQueueSize;

    public OrderedQueueSender(SendMessageExecutorManager sendMessageExecutorManager, MessageSender messageSender, ExecutorService executor) {
        this.sendMessageExecutorManager = sendMessageExecutorManager;
        this.messageSender = messageSender;

        ConfigCenter configs = ConfigCenter.getInstance();
        this.maxQueueSize = configs.getMaxQueueSize();
        // 由于需要使用 Deque PutFirst 的特性, 不能限制 queue 的最大长度
        this.queue = new LinkedBlockingDeque<>();
        // TODO(zhenwei.liu) executor 并发度设计
        this.executor = executor;
        executor.submit(this::dispatchMessages);
    }

    // TODO(zhenwei.liu) 落库成功, 入队失败的消息会乱序, 考虑数据库扫描任务对 Ordered Message 的处理?
    @Override
    public boolean offer(ProduceMessage pm) {
        if (queue.size() > maxQueueSize) {
            return false;
        }
        return queue.offer(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, long millisecondWait) {
        if (queue.size() > maxQueueSize) {
            return false;
        }
        try {
            return queue.offer(pm, millisecondWait, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            logger.error("消息入队失败 {} {} {}", pm.getSubject(), pm.getMessageId(), t.getMessage());
            return false;
        }
    }

    private void dispatchMessages() {
        while (true) {
            ProduceMessage message;
            try {
                message = queue.take();
            } catch (InterruptedException e) {
                logger.error("消息派发失败", e);
                continue;
            }
            try {
                sendMessageExecutorManager.getExecutor(message).addMessage(message);
            } catch (Throwable t) {
                try {
                    logger.error("消息派发失败", t);
                    queue.putFirst(message);
                } catch (InterruptedException e) {
                    // queue 没有长度限制, 基本是不会发生的
                    logger.error("消息重新入队失败", e);
                }
            }
        }
    }

    @Override
    public void send(ProduceMessage pm) {
        SendMessageExecutor executor = sendMessageExecutorManager.getExecutor(pm);
        messageSender.send(Collections.singletonList(pm), executor, sendMessageExecutorManager);
    }

    @Override
    public void destroy() {
        this.executor.shutdown();
    }
}
