package qunar.tc.qmq.protocol.consumer;

import qunar.tc.qmq.ConsumeStrategy;

import java.util.List;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class PullRequestV10 extends AbstractPullRequest {

    private final int consumerAllocationVersion;

    public PullRequestV10(String partitionName,
                          String group,
                          int requestNum,
                          long timeoutMillis,
                          long offset,
                          long pullOffsetBegin,
                          long pullOffsetLast,
                          String consumerId,
                          ConsumeStrategy consumeStrategy,
                          List<PullFilter> filters,
                          int consumerAllocationVersion) {
        super(partitionName, group, requestNum, timeoutMillis, offset, pullOffsetBegin, pullOffsetLast, consumerId, Objects.equals(ConsumeStrategy.EXCLUSIVE, consumeStrategy), filters);
        this.consumerAllocationVersion = consumerAllocationVersion;
    }

    public int getConsumerAllocationVersion() {
        return consumerAllocationVersion;
    }
}
