package qunar.tc.qmq.batch;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 * 顺序任务执行器, 原则上保证只有队列前的 Item 处理成功, 才继续处理下一个 Item, 流程如下
 * 1. 每次处理一批 Item, 并将状态改为 Running
 * 2. 当 Item 处理完, 将状态改为 Idle, 并触发下一次 Item 处理
 * 3. 使用 Callback 判断处理成功的 Item, 并将处理成功的 Item 移除
 * 4. Callback 过程中遇到任务执行失败, 则立即退出, 以保证有 Item 处理失败后不会继续执行
 * </pre>
 *
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedSendMessageExecutor extends StatefulSendMessageExecutor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(OrderedSendMessageExecutor.class);

    private MessageGroup messageGroup;
    private int batchSize;
    private LinkedBlockingQueue<ProduceMessage> queue;
    private MessageProcessor processor;
    private ExecutorService executor;

    public OrderedSendMessageExecutor(MessageGroup messageGroup, int batchSize, int queueSize, MessageProcessor processor, ExecutorService executor) {
        this.messageGroup = messageGroup;
        this.batchSize = batchSize;
        this.queue = new LinkedBlockingQueue<>(queueSize);
        this.processor = processor;
        this.executor = executor;
    }

    @Override
    public MessageGroup getMessageGroup() {
        return messageGroup;
    }

    @Override
    public boolean addMessage(ProduceMessage message) {
        boolean offer = this.queue.offer(message);
        if (offer) {
            this.executor.execute(this);
        }
        return offer;
    }

    @Override
    public boolean addMessage(ProduceMessage message, long timeoutMills) {
        boolean offer = false;
        try {
            offer = this.queue.offer(message, timeoutMills, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("消息入队失败 {} {} {}", message.getSubject(), message.getMessageId(), e.getMessage());
        }
        if (offer) {
            this.executor.execute(this);
        }
        return offer;
    }

    @Override
    public boolean removeMessage(ProduceMessage message) {
        return queue.remove(message);
    }

    @Override
    public void run() {
        while (!this.queue.isEmpty() && Objects.equals(getState(), Status.IDLE)) {
            if (compareAndSetState(Status.IDLE, Status.RUNNING)) {
                List<ProduceMessage> messages = Lists.newArrayListWithCapacity(batchSize);
                int count = 0;
                for (ProduceMessage message : queue) {
                    if (count < batchSize) {
                        messages.add(message);
                    }
                    count++;
                }
                if (messages.size() > 0) {
                    this.processor.process(messages, this);
                }
            }
        }
    }

    public void reset() {
        if (compareAndSetState(Status.RUNNING, Status.IDLE)) {
            // 触发一次任务
            // TODO(zhenwei.liu) 当 Client 未收到 Server 端反馈, 导致重复发送一组消息, 可能造成消息乱序, 如 ([1,2,3] 重发 [1,2,3])
            this.executor.execute(this);
        }
    }
}
