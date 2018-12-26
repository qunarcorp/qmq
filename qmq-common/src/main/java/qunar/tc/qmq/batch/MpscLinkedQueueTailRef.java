package qunar.tc.qmq.batch;

import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Created by zhaohui.yu
 * $ {DATE}
 */
abstract class MpscLinkedQueueTailRef<E> extends MpscLinkedQueuePad1<E> {

    private static final long serialVersionUID = 8717072462993327429L;

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<MpscLinkedQueueTailRef, MpscLinkedQueueNode> UPDATER;

    static {
        @SuppressWarnings("rawtypes")
        AtomicReferenceFieldUpdater<MpscLinkedQueueTailRef, MpscLinkedQueueNode> updater;
        updater = PlatformDependent.newAtomicReferenceFieldUpdater(MpscLinkedQueueTailRef.class, "tailRef");
        if (updater == null) {
            updater = AtomicReferenceFieldUpdater.newUpdater(
                    MpscLinkedQueueTailRef.class, MpscLinkedQueueNode.class, "tailRef");
        }
        UPDATER = updater;
    }

    private transient volatile MpscLinkedQueueNode<E> tailRef;

    protected final MpscLinkedQueueNode<E> tailRef() {
        return tailRef;
    }

    protected final void setTailRef(MpscLinkedQueueNode<E> tailRef) {
        this.tailRef = tailRef;
    }

    @SuppressWarnings("unchecked")
    protected final MpscLinkedQueueNode<E> getAndSetTailRef(MpscLinkedQueueNode<E> tailRef) {
        // LOCK XCHG in JDK8, a CAS loop in JDK 7/6
        return (MpscLinkedQueueNode<E>) UPDATER.getAndSet(this, tailRef);
    }
}
