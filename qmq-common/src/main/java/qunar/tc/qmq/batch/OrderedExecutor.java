package qunar.tc.qmq.batch;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
public class OrderedExecutor<Item> implements Runnable {

    public enum Status {
        IDLE, RUNNING
    }

    private AtomicReference<Status> status = new AtomicReference<>(Status.IDLE);
    private String name;
    private int batchSize;
    private LinkedBlockingQueue<Item> queue;
    private OrderedProcessor<Item> processor;
    private ThreadPoolExecutor executor;

    public OrderedExecutor(String name, int batchSize, int queueSize, OrderedProcessor<Item> processor, ThreadPoolExecutor executor) {
        this.name = name;
        this.batchSize = batchSize;
        this.queue = new LinkedBlockingQueue<>(queueSize);
        this.processor = processor;
        this.executor = executor;
    }

    public boolean addItem(Item item) {
        boolean offer = this.queue.offer(item);
        if (offer) {
            this.executor.execute(this);
        }
        return offer;
    }

    public boolean addItem(Item item, long timeout, TimeUnit unit) throws InterruptedException {
        boolean offer = this.queue.offer(item, timeout, unit);
        if (offer) {
            this.executor.execute(this);
        }
        return offer;
    }

    @Override
    public void run() {
        while (!this.queue.isEmpty() && Objects.equals(status.get(), Status.IDLE)) {
            if (status.compareAndSet(Status.IDLE, Status.RUNNING)) {
                List<Item> requestList = Lists.newArrayListWithCapacity(batchSize);
                int count = 0;
                for (Item item : queue) {
                    if (count < batchSize) {
                        requestList.add(item);
                    }
                    count++;
                }
                if (requestList.size() > 0) {
                    this.processor.process(requestList, this);
                }
            }
        }
    }

    public boolean removeItem(Item item) {
        return queue.remove(item);
    }

    public void reset() {
        if (status.compareAndSet(OrderedExecutor.Status.RUNNING, OrderedExecutor.Status.IDLE)) {
            // 触发一次任务
            // TODO(zhenwei.liu) 当 Client 未收到 Server 端反馈, 导致重复发送一组消息, 可能造成消息乱序, 如 ([1,2,3] 重发 [1,2,3])
            this.executor.execute(this);
        }
    }
}
