package qunar.tc.qmq.batch;

import io.netty.util.internal.PlatformDependent;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Created by zhaohui.yu
 * $ {DATE}
 */
abstract class MpscLinkedQueueHeadRef<E> extends MpscLinkedQueuePad0<E> implements Serializable {

    private static final long serialVersionUID = 8467054865577874285L;

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<MpscLinkedQueueHeadRef, MpscLinkedQueueNode> UPDATER;

    static {
        @SuppressWarnings("rawtypes")
        AtomicReferenceFieldUpdater<MpscLinkedQueueHeadRef, MpscLinkedQueueNode> updater;
        updater = PlatformDependent.newAtomicReferenceFieldUpdater(MpscLinkedQueueHeadRef.class, "headRef");
        if (updater == null) {
            updater = AtomicReferenceFieldUpdater.newUpdater(
                    MpscLinkedQueueHeadRef.class, MpscLinkedQueueNode.class, "headRef");
        }
        UPDATER = updater;
    }

    private transient  volatile MpscLinkedQueueNode<E> headRef;

    protected final MpscLinkedQueueNode<E> headRef() {
        return headRef;
    }

    protected final void setHeadRef(MpscLinkedQueueNode<E> headRef) {
        this.headRef = headRef;
    }

    protected final void lazySetHeadRef(MpscLinkedQueueNode<E> headRef) {
        UPDATER.lazySet(this, headRef);
    }
}
