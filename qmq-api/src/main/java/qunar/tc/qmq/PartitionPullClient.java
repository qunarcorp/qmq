package qunar.tc.qmq;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface PartitionPullClient extends PullClient {

    int getPartition();
}
