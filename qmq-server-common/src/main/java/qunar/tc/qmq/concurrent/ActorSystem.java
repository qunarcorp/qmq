/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.concurrent;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.monitor.QMon;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaohui.yu
 * 16/4/8
 */
public class ActorSystem {
    private static final Logger LOG = LoggerFactory.getLogger(ActorSystem.class);

    private static final int DEFAULT_QUEUE_SIZE = 10000;

    private final ConcurrentMap<String, Actor> actors;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger actorsCount;
    private final String name;

    public ActorSystem(String name) {
        this(name, Runtime.getRuntime().availableProcessors() * 4, true);
    }

    public ActorSystem(String name, int threads, boolean fair) {
        this.name = name;
        this.actorsCount = new AtomicInteger();
        BlockingQueue<Runnable> queue = fair ? new PriorityBlockingQueue<>() : new LinkedBlockingQueue<>();
        this.executor = new ThreadPoolExecutor(threads, threads, 60, TimeUnit.MINUTES, queue, new NamedThreadFactory("actor-sys-" + name));
        this.actors = Maps.newConcurrentMap();
        QMon.dispatchersGauge(name, actorsCount::doubleValue);
        QMon.actorSystemQueueGauge(name, () -> (double) executor.getQueue().size());
    }

    public <E> void dispatch(String actorPath, E msg, Processor<E> processor) {
        Actor<E> actor = createOrGet(actorPath, processor);
        actor.dispatch(msg);
        schedule(actor, true);
    }

    public void suspend(String actorPath) {
        Actor actor = actors.get(actorPath);
        if (actor == null) return;

        actor.suspend();
    }

    public void resume(String actorPath) {
        Actor actor = actors.get(actorPath);
        if (actor == null) return;

        actor.resume();
        schedule(actor, false);
    }

    private <E> Actor<E> createOrGet(String actorPath, Processor<E> processor) {
        Actor<E> actor = actors.get(actorPath);
        if (actor != null) return actor;

        Actor<E> add = new Actor<>(this.name, actorPath, this, processor, DEFAULT_QUEUE_SIZE);
        Actor<E> old = actors.putIfAbsent(actorPath, add);
        if (old == null) {
            LOG.info("create actorSystem: {}", actorPath);
            actorsCount.incrementAndGet();
            return add;
        }
        return old;
    }

    private <E> boolean schedule(Actor<E> actor, boolean hasMessageHint) {
        if (!actor.canBeSchedule(hasMessageHint)) return false;
        if (actor.setAsScheduled()) {
            actor.submitTs = System.currentTimeMillis();
            this.executor.execute(actor);
            return true;
        }
        return false;
    }

    public interface Processor<T> {
        boolean process(T message, Actor<T> self);
    }

    public static class Actor<E> implements Runnable, Comparable<Actor> {
        private static final int Open = 0;
        private static final int Scheduled = 2;
        private static final int shouldScheduleMask = 3;
        private static final int shouldNotProcessMask = ~2;
        private static final int suspendUnit = 4;
        //每个actor至少执行的时间片
        private static final int QUOTA = 5;
        private static long statusOffset;

        static {
            try {
                statusOffset = Unsafe.instance.objectFieldOffset(Actor.class.getDeclaredField("status"));
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }

        final String systemName;
        final ActorSystem actorSystem;
        final BoundedNodeQueue<E> queue;
        final Processor<E> processor;
        private final String name;
        private long total;
        private volatile long submitTs;
        //通过Unsafe操作
        private volatile int status;

        Actor(String systemName, String name, ActorSystem actorSystem, Processor<E> processor, final int queueSize) {
            this.systemName = systemName;
            this.name = name;
            this.actorSystem = actorSystem;
            this.processor = processor;
            this.queue = new BoundedNodeQueue<>(queueSize);

            QMon.actorQueueGauge(systemName, name, () -> (double) queue.count());
        }

        boolean dispatch(E message) {
            return queue.add(message);
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            String old = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(systemName + "-" + name);
                if (shouldProcessMessage()) {
                    processMessages();
                }
            } finally {
                long duration = System.currentTimeMillis() - start;
                total += duration;
                QMon.actorProcessTime(name, duration);

                Thread.currentThread().setName(old);
                setAsIdle();
                this.actorSystem.schedule(this, false);
            }
        }

        void processMessages() {
            long deadline = System.currentTimeMillis() + QUOTA;
            while (true) {
                E message = queue.peek();
                if (message == null) return;
                boolean process = processor.process(message, this);
                if (!process) return;

                queue.pollNode();
                if (System.currentTimeMillis() >= deadline) return;
            }
        }

        final boolean shouldProcessMessage() {
            return (currentStatus() & shouldNotProcessMask) == 0;
        }

        private boolean canBeSchedule(boolean hasMessageHint) {
            int s = currentStatus();
            if (s == Open || s == Scheduled) return hasMessageHint || !queue.isEmpty();
            return false;
        }

        public final boolean resume() {
            while (true) {
                int s = currentStatus();
                int next = s < suspendUnit ? s : s - suspendUnit;
                if (updateStatus(s, next)) return next < suspendUnit;
            }
        }

        public final void suspend() {
            while (true) {
                int s = currentStatus();
                if (updateStatus(s, s + suspendUnit)) return;
            }
        }

        final boolean setAsScheduled() {
            while (true) {
                int s = currentStatus();
                if ((s & shouldScheduleMask) != Open) return false;
                if (updateStatus(s, s | Scheduled)) return true;
            }
        }

        final void setAsIdle() {
            while (true) {
                int s = currentStatus();
                if (updateStatus(s, s & ~Scheduled)) return;
            }
        }

        final int currentStatus() {
            return Unsafe.instance.getIntVolatile(this, statusOffset);
        }

        private boolean updateStatus(int oldStatus, int newStatus) {
            return Unsafe.instance.compareAndSwapInt(this, statusOffset, oldStatus, newStatus);
        }

        @Override
        public int compareTo(Actor o) {
            int result = Long.compare(total, o.total);
            return result == 0 ? Long.compare(submitTs, o.submitTs) : result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Actor<?> actor = (Actor<?>) o;
            return Objects.equals(systemName, actor.systemName) &&
                    Objects.equals(name, actor.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(systemName, name);
        }
    }

    /**
     * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
     */

    /**
     * Lock-free bounded non-blocking multiple-producer single-consumer queue based on the works of:
     * <p/>
     * Andriy Plokhotnuyk (https://github.com/plokhotnyuk)
     * - https://github.com/plokhotnyuk/actors/blob/2e65abb7ce4cbfcb1b29c98ee99303d6ced6b01f/src/test/scala/akka/dispatch/Mailboxes.scala
     * (Apache V2: https://github.com/plokhotnyuk/actors/blob/master/LICENSE)
     * <p/>
     * Dmitriy Vyukov's non-intrusive MPSC queue:
     * - http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
     * (Simplified BSD)
     */
    @SuppressWarnings("serial")
    private static class BoundedNodeQueue<T> {

        private final static long enqOffset, deqOffset;

        static {
            try {
                enqOffset = Unsafe.instance.objectFieldOffset(BoundedNodeQueue.class.getDeclaredField("_enqDoNotCallMeDirectly"));
                deqOffset = Unsafe.instance.objectFieldOffset(BoundedNodeQueue.class.getDeclaredField("_deqDoNotCallMeDirectly"));
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }

        private final int capacity;
        @SuppressWarnings("unused")
        private volatile Node<T> _enqDoNotCallMeDirectly;
        @SuppressWarnings("unused")
        private volatile Node<T> _deqDoNotCallMeDirectly;

        protected BoundedNodeQueue(final int capacity) {
            if (capacity < 0) throw new IllegalArgumentException("AbstractBoundedNodeQueue.capacity must be >= 0");
            this.capacity = capacity;
            final Node<T> n = new Node<T>();
            setDeq(n);
            setEnq(n);
        }

        @SuppressWarnings("unchecked")
        private Node<T> getEnq() {
            return (Node<T>) Unsafe.instance.getObjectVolatile(this, enqOffset);
        }

        private void setEnq(Node<T> n) {
            Unsafe.instance.putObjectVolatile(this, enqOffset, n);
        }

        private boolean casEnq(Node<T> old, Node<T> nju) {
            return Unsafe.instance.compareAndSwapObject(this, enqOffset, old, nju);
        }

        @SuppressWarnings("unchecked")
        private Node<T> getDeq() {
            return (Node<T>) Unsafe.instance.getObjectVolatile(this, deqOffset);
        }

        private void setDeq(Node<T> n) {
            Unsafe.instance.putObjectVolatile(this, deqOffset, n);
        }

        private boolean casDeq(Node<T> old, Node<T> nju) {
            return Unsafe.instance.compareAndSwapObject(this, deqOffset, old, nju);
        }

        public final int count() {
            final Node<T> lastNode = getEnq();
            final int lastNodeCount = lastNode.count;
            return lastNodeCount - getDeq().count;
        }

        /**
         * @return the maximum capacity of this queue
         */
        public final int capacity() {
            return capacity;
        }

        // Possible TODO — impl. could be switched to addNode(new Node(value)) if we want to allocate even if full already
        public final boolean add(final T value) {
            for (Node<T> n = null; ; ) {
                final Node<T> lastNode = getEnq();
                final int lastNodeCount = lastNode.count;
                if (lastNodeCount - getDeq().count < capacity) {
                    // Trade a branch for avoiding to create a new node if full,
                    // and to avoid creating multiple nodes on write conflict á la Be Kind to Your GC
                    if (n == null) {
                        n = new Node<T>();
                        n.value = value;
                    }

                    n.count = lastNodeCount + 1; // Piggyback on the HB-edge between getEnq() and casEnq()

                    // Try to putPullLogs the node to the end, if we fail we continue loopin'
                    if (casEnq(lastNode, n)) {
                        lastNode.setNext(n);
                        return true;
                    }
                } else return false; // Over capacity—couldn't add the node
            }
        }

        public final boolean isEmpty() {
            return getEnq() == getDeq();
        }

        /**
         * Removes the first element of this queue if any
         *
         * @return the value of the first element of the queue, null if empty
         */
        public final T poll() {
            final Node<T> n = pollNode();
            return (n != null) ? n.value : null;
        }

        public final T peek() {
            Node<T> n = peekNode();
            return (n != null) ? n.value : null;
        }

        @SuppressWarnings("unchecked")
        protected final Node<T> peekNode() {
            for (; ; ) {
                final Node<T> deq = getDeq();
                final Node<T> next = deq.next();
                if (next != null || getEnq() == deq)
                    return next;
            }
        }

        /**
         * Removes the first element of this queue if any
         *
         * @return the `Node` of the first element of the queue, null if empty
         */
        public final Node<T> pollNode() {
            for (; ; ) {
                final Node<T> deq = getDeq();
                final Node<T> next = deq.next();
                if (next != null) {
                    if (casDeq(deq, next)) {
                        deq.value = next.value;
                        deq.setNext(null);
                        next.value = null;
                        return deq;
                    } // else we retry (concurrent consumers)
                } else if (getEnq() == deq) return null; // If we got a null and head meets tail, we are empty
            }
        }

        public static class Node<T> {
            private final static long nextOffset;

            static {
                try {
                    nextOffset = Unsafe.instance.objectFieldOffset(Node.class.getDeclaredField("_nextDoNotCallMeDirectly"));
                } catch (Throwable t) {
                    throw new ExceptionInInitializerError(t);
                }
            }

            protected T value;
            protected int count;
            @SuppressWarnings("unused")
            private volatile Node<T> _nextDoNotCallMeDirectly;

            @SuppressWarnings("unchecked")
            public final Node<T> next() {
                return (Node<T>) Unsafe.instance.getObjectVolatile(this, nextOffset);
            }

            protected final void setNext(final Node<T> newNext) {
                Unsafe.instance.putOrderedObject(this, nextOffset, newNext);
            }
        }
    }

    static class Unsafe {
        public final static sun.misc.Unsafe instance;

        static {
            try {
                sun.misc.Unsafe found = null;
                for (Field field : sun.misc.Unsafe.class.getDeclaredFields()) {
                    if (field.getType() == sun.misc.Unsafe.class) {
                        field.setAccessible(true);
                        found = (sun.misc.Unsafe) field.get(null);
                        break;
                    }
                }
                if (found == null) throw new IllegalStateException("Can't find instance of sun.misc.Unsafe");
                else instance = found;
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }
    }
}
