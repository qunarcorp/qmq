package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 2020/6/7
 */
public class PullLogIndexEntry {
    public final long startOfPullLogSequence;

    public final long baseOfMessageSequence;

    public final int position;

    public PullLogIndexEntry(final long startOfPullLogSequence, final long baseOfMessageSequence, int position) {
        this.startOfPullLogSequence = startOfPullLogSequence;
        this.baseOfMessageSequence = baseOfMessageSequence;
        this.position = position;
    }

}
