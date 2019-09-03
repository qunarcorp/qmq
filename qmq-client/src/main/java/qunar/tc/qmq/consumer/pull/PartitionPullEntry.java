package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.PartitionPullClient;
import qunar.tc.qmq.PullEntry;
import qunar.tc.qmq.StatusSource;

import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class PartitionPullEntry implements PullEntry, PartitionPullClient {

    private int physicalPartition;
    private PullEntry pullEntry;

    public PartitionPullEntry(int physicalPartition, PullEntry pullEntry) {
        this.physicalPartition = physicalPartition;
        this.pullEntry = pullEntry;
    }

    @Override
    public int getPartition() {
        return physicalPartition;
    }

    @Override
    public void online(StatusSource statusSource) {
        pullEntry.online(statusSource);
    }

    @Override
    public void offline(StatusSource statusSource) {
        pullEntry.offline(statusSource);
    }

    @Override
    public void startPull(ExecutorService executor) {
        pullEntry.startPull(executor);
    }

    @Override
    public void destroy() {
        pullEntry.destroy();
        // TODO(zhenwei.liu) 这里需要主动释放锁
    }
}
