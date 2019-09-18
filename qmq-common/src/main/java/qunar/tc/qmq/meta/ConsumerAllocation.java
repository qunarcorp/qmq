package qunar.tc.qmq.meta;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.Versionable;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ConsumerAllocation implements Versionable {

    private int allocationVersion;
    private List<PartitionProps> partitionProps;
    private long expired; // 本次心跳授权超时时间
    private ConsumeStrategy consumeStrategy;

    public ConsumerAllocation(int allocationVersion, List<PartitionProps> partitionProps, long expired, ConsumeStrategy consumeStrategy) {
        this.allocationVersion = allocationVersion;
        this.partitionProps = partitionProps;
        this.expired = expired;
        this.consumeStrategy = consumeStrategy;
    }

    @Override
    public int getVersion() {
        return allocationVersion;
    }

    public List<PartitionProps> getPartitionProps() {
        return partitionProps;
    }

    public long getExpired() {
        return expired;
    }

    public ConsumeStrategy getConsumeStrategy() {
        return consumeStrategy;
    }

    public ConsumerAllocation getRetryConsumerAllocation(String consumerGroup) {
        List<PartitionProps> copy = new ArrayList<>(partitionProps.size());
        for (PartitionProps partitionProp : partitionProps) {
            copy.add(new PartitionProps(partitionProp.getPartitionId(), RetryPartitionUtils.buildRetryPartitionName(partitionProp.getPartitionName(), consumerGroup), partitionProp.getBrokerGroup()));
        }

        return new ConsumerAllocation(allocationVersion, copy, expired, consumeStrategy);
    }
}
