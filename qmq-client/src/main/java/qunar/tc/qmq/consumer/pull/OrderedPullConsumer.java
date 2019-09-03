package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.OrderedPullClient;
import qunar.tc.qmq.PartitionAllocation;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class OrderedPullConsumer extends CompositePullConsumer<PartitionPullConsumer> implements OrderedPullClient<PartitionPullConsumer> {

    private PartitionAllocation partitionAllocation;

    public OrderedPullConsumer(List<PartitionPullConsumer> consumers, PartitionAllocation partitionAllocation) {
        super(consumers);
        this.partitionAllocation = partitionAllocation;
    }

    @Override
    public PartitionAllocation getPartitionAllocation() {
        return partitionAllocation;
    }
}
