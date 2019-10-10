package qunar.tc.qmq.consumer.pull;

import java.util.List;
import qunar.tc.qmq.CompositePullClient;
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
            int version,
            boolean isBroadcast,
            boolean isOrdered,
            long consumptionExpiredTime,
            List<T> pullEntries) {
        super(subject, consumerGroup, "", "", consumerId, null, version, isBroadcast, isOrdered, consumptionExpiredTime,
                pullEntries);
    }
}
