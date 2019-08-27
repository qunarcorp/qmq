package qunar.tc.qmq.protocol.producer;

import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.protocol.MetaInfoResponse;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ProducerMetaInfoResponse extends MetaInfoResponse {

    private PartitionMapping partitionMapping;

    public PartitionMapping getPartitionMapping() {
        return partitionMapping;
    }

    public ProducerMetaInfoResponse setPartitionMapping(PartitionMapping partitionMapping) {
        this.partitionMapping = partitionMapping;
        return this;
    }
}
