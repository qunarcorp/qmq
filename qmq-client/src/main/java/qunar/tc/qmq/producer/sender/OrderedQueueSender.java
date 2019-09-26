package qunar.tc.qmq.producer.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.producer.ConfigCenter;
import qunar.tc.qmq.producer.QueueSender;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
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
    private final LinkedBlockingQueue<ProduceMessage> queue;
    private final SendMessageExecutorManager sendMessageExecutorManager;
    private final MessageSender messageSender;

    public OrderedQueueSender(SendMessageExecutorManager sendMessageExecutorManager, MessageSender messageSender, ExecutorService executor) {
        this.sendMessageExecutorManager = sendMessageExecutorManager;
        this.messageSender = messageSender;

        ConfigCenter configs = ConfigCenter.getInstance();
        int maxQueueSize = configs.getMaxQueueSize();
        this.queue = new LinkedBlockingQueue<>(maxQueueSize);
        // TODO(zhenwei.liu) executor 并发度设计
        this.executor = executor;
        executor.submit(this::dispatchMessages);
    }

    // TODO(zhenwei.liu) 落库成功, 入队失败的消息会乱序, 考虑数据库扫描任务对 Ordered Message 的处理?
    @Override
    public boolean offer(ProduceMessage pm) {
        return queue.offer(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, long millisecondWait) {
        try {
            return queue.offer(pm, millisecondWait, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            logger.error("消息入队失败 {} {} {}", pm.getSubject(), pm.getMessageId(), t.getMessage());
            return false;
        }
    }

    private void dispatchMessages() {
        while (true) {
            try {
                ProduceMessage message = queue.take();
                sendMessageExecutorManager.getExecutor(message).addMessage(message);
            } catch (Throwable t) {
                logger.error("消息派发失败", t);
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
