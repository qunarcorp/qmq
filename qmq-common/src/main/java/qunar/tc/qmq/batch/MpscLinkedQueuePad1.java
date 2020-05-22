package qunar.tc.qmq.batch;


/**
 * Created by zhaohui.yu
 * $ {DATE}
 */
abstract class MpscLinkedQueuePad1<E> extends MpscLinkedQueueHeadRef<E> {

    private static final long serialVersionUID = 2886694927079691637L;

    long p00, p01, p02, p03, p04, p05, p06, p07;
    long p30, p31, p32, p33, p34, p35, p36, p37;
}
