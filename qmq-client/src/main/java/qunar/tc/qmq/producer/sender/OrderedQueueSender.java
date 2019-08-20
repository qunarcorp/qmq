package qunar.tc.qmq.producer.sender;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.batch.OrderedExecutor;
import qunar.tc.qmq.batch.OrderedProcessor;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.service.exceptions.DuplicateMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 用于发送顺序消息, 对顺序消息来说, 每个 subject-physicalPartition 需要一个独立队列处理
 *
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedQueueSender extends AbstractQueueSender implements OrderedProcessor<ProduceMessage> {

    private ThreadPoolExecutor executor; // 所有分区共享一个线程池
    private int maxQueueSize;
    private int sendBatch;

    // subject-physicalPartition => executor
    private Map<String, OrderedExecutor<ProduceMessage>> executorMap = Maps.newConcurrentMap();

    @Override
    public void init(Map<PropKey, Object> props) {
        String name = (String) Preconditions.checkNotNull(props.get(PropKey.SENDER_NAME));
        int sendThreads = (int) Preconditions.checkNotNull(props.get(PropKey.SEND_THREADS));
        this.timer = Metrics.timer("qmq_client_send_ordered_task_timer");
        this.maxQueueSize = (int) Preconditions.checkNotNull(props.get(PropKey.MAX_QUEUE_SIZE));
        this.sendBatch = (int) Preconditions.checkNotNull(props.get(PropKey.SEND_BATCH));
        this.routerManager = (RouterManager) Preconditions.checkNotNull(props.get(PropKey.ROUTER_MANAGER));
        this.executor = new ThreadPoolExecutor(1, sendThreads, 1L, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(1), new NamedThreadFactory("batch-ordered-" + name + "-task", true));
        this.executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
    }

    @Override
    public boolean offer(ProduceMessage pm) {
        return getExecutor(pm).addItem(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, long millisecondWait) {
        boolean inserted;
        try {
            inserted = getExecutor(pm).addItem(pm, millisecondWait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
        return inserted;
    }

    /**
     * 顺序消息不支持同步等待, 必须入队异步执行
     *
     * @param pm 消息
     */
    @Override
    public void send(ProduceMessage pm) {
        this.offer(pm);
    }

    @Override
    public void destroy() {
        this.executor.shutdown();
    }

    @Override
    public void error(ProduceMessage pm, Exception e) {
        // TODO
    }

    @Override
    public void failed(ProduceMessage pm, Exception e) {
        // TODO
    }

    @Override
    public void block(ProduceMessage pm, MessageException ex) {
        // TODO
    }

    @Override
    public void finish(ProduceMessage pm, Exception e) {
        // TODO
    }

    private OrderedExecutor<ProduceMessage> getExecutor(ProduceMessage produceMessage) {
        BaseMessage baseMessage = (BaseMessage) produceMessage.getBase();
        String key = baseMessage.getSubject() + "#" + baseMessage.getStringProperty(keys.qmq_physicalPartition);
        return executorMap.computeIfAbsent(key, k -> new OrderedExecutor<>(k, sendBatch, maxQueueSize, this, executor));
    }

    @Override
    public void process(List<ProduceMessage> produceMessages, OrderedExecutor<ProduceMessage> executor) {
        long start = System.currentTimeMillis();
        try {
            //按照路由分组发送
            Collection<MessageSenderGroup> messages = groupBy(produceMessages);
            for (MessageSenderGroup group : messages) {
                QmqTimer timer = Metrics.timer("qmq_client_producer_send_broker_time");
                long startTime = System.currentTimeMillis();
                group.send((source, result) -> executor.onCallback(produceMessages, produceMessage -> {
                    // 在这里判断最终是否发送成功, 发送成功的消息会别 executor 从 queue 中移除
                    // 连续成功机制由 broker 端来控制
                    MessageException ex = result.get(produceMessage.getMessageId());
                    return ex == null || ex instanceof DuplicateMessageException;
                }));
                timer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            }
        } finally {
            timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
    }
}
