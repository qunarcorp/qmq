package qunar.tc.qmq;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface OrderedPullClient<T extends PartitionPullClient> extends CompositePullClient<T> {

     ConsumerAllocation getConsumerAllocation();
}
