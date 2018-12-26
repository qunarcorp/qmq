package qunar.tc.qmq.batch;

import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Created by zhaohui.yu
 * $ {DATE}
 */
abstract class MpscLinkedQueueNode<T> {

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<MpscLinkedQueueNode, MpscLinkedQueueNode> nextUpdater;

    static {
        @SuppressWarnings("rawtypes")
        AtomicReferenceFieldUpdater<MpscLinkedQueueNode, MpscLinkedQueueNode> u;

        u = PlatformDependent.newAtomicReferenceFieldUpdater(MpscLinkedQueueNode.class, "next");
        if (u == null) {
            u = AtomicReferenceFieldUpdater.newUpdater(MpscLinkedQueueNode.class, MpscLinkedQueueNode.class, "next");
        }
        nextUpdater = u;
    }

    @SuppressWarnings("unused")
    private volatile MpscLinkedQueueNode<T> next;

    final MpscLinkedQueueNode<T> next() {
        return next;
    }

    final void setNext(final MpscLinkedQueueNode<T> newNext) {
        // Similar to 'next = newNext', but slightly faster (storestore vs loadstore)
        // See: http://robsjava.blogspot.com/2013/06/a-faster-volatile.html
        nextUpdater.lazySet(this, newNext);
    }

    public abstract T value();

    /**
     * Sets the element this node contains to {@code null} so that the node can be used as a tombstone.
     */
    protected T clearMaybe() {
        return value();
    }

    /**
     * Unlink to allow GC'ed
     */
    void unlink() {
        setNext(null);
    }
}
