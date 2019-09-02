package qunar.tc.qmq.consumer.pull;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface PartitionPullClient extends PullClient {

    int getPartition();
}
