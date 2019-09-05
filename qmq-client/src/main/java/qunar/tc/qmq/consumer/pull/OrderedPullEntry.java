package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.ConsumerAllocation;
import qunar.tc.qmq.OrderedPullClient;
import qunar.tc.qmq.PartitionAllocation;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class OrderedPullEntry extends CompositePullEntry<PartitionPullEntry> implements OrderedPullClient<PartitionPullEntry> {

    private ConsumerAllocation consumerAllocation;

    public OrderedPullEntry(List<PartitionPullEntry> pullEntries, ConsumerAllocation consumerAllocation) {
        super(pullEntries);
        this.consumerAllocation = consumerAllocation;
    }

    @Override
    public ConsumerAllocation getConsumerAllocation() {
        return consumerAllocation;
    }
}
