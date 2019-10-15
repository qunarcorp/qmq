package qunar.tc.qmq.consumer.pull;

import java.util.List;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullEntry;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class CompositePullEntry<T extends PullEntry> extends AbstractCompositePullClient<T> implements PullEntry,
        CompositePullClient<T> {

    public CompositePullEntry(
            String subject,
            String consumerGroup,
            String consumerId,
            ConsumeStrategy consumeStrategy,
            int allocationVersion,
            boolean isBroadcast,
            boolean isOrdered,
            int partitionSetVersion,
            long consumptionExpiredTime,
            List<T> pullEntries) {
        super(subject, consumerGroup, "", consumerId, consumeStrategy, allocationVersion, isBroadcast, isOrdered,
                partitionSetVersion, consumptionExpiredTime, pullEntries);
    }
}
