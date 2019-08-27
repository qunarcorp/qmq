package qunar.tc.qmq.protocol.consumer;

import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.protocol.MetaInfoResponse;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ConsumerMetaInfoResponse extends MetaInfoResponse {

    private PartitionAllocation partitionAllocation;

    public PartitionAllocation getPartitionAllocation() {
        return partitionAllocation;
    }

    public ConsumerMetaInfoResponse setPartitionAllocation(PartitionAllocation partitionAllocation) {
        this.partitionAllocation = partitionAllocation;
        return this;
    }
}
