package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.meta.PartitionAllocation;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class OrderedPullEntry extends CompositePullEntry<PartitionPullEntry> implements OrderedPullClient<PartitionPullEntry> {

    private PartitionAllocation partitionAllocation;

    public OrderedPullEntry(List<PartitionPullEntry> pullEntries, PartitionAllocation partitionAllocation) {
        super(pullEntries);
        this.partitionAllocation = partitionAllocation;
    }

    @Override
    public PartitionAllocation getPartitionAllocation() {
        return partitionAllocation;
    }
}
