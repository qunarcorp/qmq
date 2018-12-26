package qunar.tc.qmq.batch;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.concurrent.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * User: zhaohuiyu
 * Date: 2/5/15
 * Time: 2:46 PM
 */
public class MultipleQueueFlusher<Item> {

    private final LoadBalance<Item> loadBalance;

    private final Queue<Item>[] flusherQueues;

    private int flushInterval;

    private Future[] futures;

    private static final Random r = new Random();

    private final ExecutorService EXECUTORS = Executors.newCachedThreadPool(new NamedThreadFactory("MultipleQueueFlusher"));

    private final ScheduledExecutorService SCHEDULE = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("MultipleQueueFlusher-SCHED"));

    private static final int delta = 50;

    public MultipleQueueFlusher(String name, int queues, int capacityPerQueue, int batch, int flushInterval, int threads, LoadBalance<Item> loadBalance, Processor<Item> processor) {
        this.flusherQueues = new Queue[queues];
        this.futures = new Future[queues];

        Flusher<Item>[] flushers = new Flusher[threads];
        this.loadBalance = loadBalance;
        this.flushInterval = flushInterval;
        for (int i = 0; i < threads; ++i) {
            flushers[i] = new Flusher<Item>(capacityPerQueue, batch, flushInterval, processor);
        }

        for (int i = 0; i < queues; ++i) {
            int flusherIndex = i % threads;
            final Queue<Item> queue = flushers[flusherIndex].create(i, name);

            this.flusherQueues[i] = queue;
        }

        for (int i = 0; i < threads; ++i) {
            EXECUTORS.submit(flushers[i]);
        }

        schedule();
    }

    private void schedule() {
        for (int i = 0; i < flusherQueues.length; ++i) {
            final Queue<Item> queue = flusherQueues[i];
            futures[i] = SCHEDULE.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    queue.timeout();
                }
            }, r.nextInt(delta), flushInterval, TimeUnit.MILLISECONDS);
        }
    }

    public boolean add(Item item) {
        int index = loadBalance.idx(item);
        return flusherQueues[index].add(item);
    }

    public void setFlushInterval(int flushInterval) {
        Preconditions.checkArgument(flushInterval > 0);

        if (this.flushInterval == flushInterval) return;
        this.flushInterval = flushInterval;
        for (int i = 0; i < futures.length; ++i) {
            futures[i].cancel(false);
            final Queue<Item> queue = flusherQueues[i];
            queue.setFlushInterval(flushInterval);
            futures[i] = SCHEDULE.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    queue.timeout();
                }
            }, r.nextInt(delta), flushInterval, TimeUnit.MILLISECONDS);
        }
    }

    public void setBatch(int batch) {
        for (int i = 0; i < flusherQueues.length; ++i) {
            this.flusherQueues[i].setBatch(batch);
        }
    }

    private static class Flusher<Item> implements Runnable {
        private final int capacityPerQueue;
        private final int batch;
        private final int flushInterval;
        private final Processor<Item> processor;

        private List<Queue<Item>> queues;

        private final Object notifier;

        Flusher(int capacityPerQueue, int batch, int flushInterval, Processor<Item> processor) {
            this.capacityPerQueue = capacityPerQueue;
            this.batch = batch;
            this.flushInterval = flushInterval;
            this.processor = processor;
            this.notifier = new Object();
            this.queues = new ArrayList<Queue<Item>>();
        }


        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                select();
                process();
            }
        }

        private void select() {
            synchronized (notifier) {
                while (!flushOnDemand()) {
                    waitAWhile();
                }
            }
        }

        private void process() {
            while (!Thread.currentThread().isInterrupted()) {
                boolean poll = false;
                for (int i = 0; i < queues.size(); ++i) {
                    Queue<Item> queue = queues.get(i);
                    if (queue.canFlush()) {
                        queue.flush();
                        poll = true;
                    }
                }
                if (!poll) break;
            }
        }

        private boolean flushOnDemand() {
            for (int i = 0; i < queues.size(); ++i) {
                Queue<Item> queue = queues.get(i);
                if (queue.canFlush()) {
                    return true;
                }
            }
            return false;
        }

        private void waitAWhile() {
            try {
                notifier.wait();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }

        public Queue<Item> create(int index, String name) {
            Queue<Item> queue = new Queue<Item>(name, index, capacityPerQueue, batch, flushInterval, notifier, processor);
            this.queues.add(queue);
            return queue;
        }
    }

    private static class Queue<Item> {
        private final int index;
        private final Processor<Item> processor;

        private volatile int flushInterval;

        private volatile int batch;

        private volatile long lastFlush = System.currentTimeMillis();

        private MpscLinkedQueue<Item> queue;

        private final Object notifier;

        Queue(String name, int index, int capacity, int batch, int flushInterval, Object notifier, Processor<Item> processor) {
            this.index = index;
            this.batch = batch;
            this.flushInterval = flushInterval;
            this.notifier = notifier;
            this.processor = processor;

            this.queue = new MpscLinkedQueue<Item>(capacity);
        }

        public boolean add(Item item) {
            boolean entered = this.queue.offer(item);
            flushOnDemand();
            return entered;
        }

        public void flush() {
            lastFlush = System.currentTimeMillis();
            List<Item> items = new ArrayList<Item>(batch);
            int size = queue.drainTo(items, batch);
            if (size > 0) {
                processor.process(new Group<Item>(index, items));
            }
        }

        private void flushOnDemand() {
            if (queue.SIZE() < batch) return;
            start();
        }

        private void start() {
            synchronized (notifier) {
                notifier.notify();
            }
        }

        boolean canFlush() {
            return (queue.SIZE() >= batch) || (System.currentTimeMillis() - lastFlush >= flushInterval);
        }

        public void timeout() {
            if (System.currentTimeMillis() - lastFlush < flushInterval) return;
            start();
        }

        void setFlushInterval(int flushInterval) {
            this.flushInterval = flushInterval;
        }

        public void setBatch(int batch) {
            this.batch = batch;
        }
    }

    public static class Group<Item> {
        public final int index;

        public final List<Item> items;

        Group(int index, List<Item> items) {
            this.index = index;
            this.items = items;
        }
    }

    public interface Processor<Item> {
        void process(Group<Item> group);
    }

    public interface LoadBalance<Item> {
        int idx(Item item);
    }
}
