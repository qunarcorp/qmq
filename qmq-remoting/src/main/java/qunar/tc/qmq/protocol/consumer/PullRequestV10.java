package qunar.tc.qmq.protocol.consumer;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class PullRequestV10 extends PullRequest {

    private int consumerAllocationVersion;

    public PullRequestV10(String subject, String group, int requestNum, long timeoutMillis, long offset, long pullOffsetBegin, long pullOffsetLast, String consumerId, boolean isExclusiveConsume, List<PullFilter> filters, int consumerAllocationVersion) {
        super(subject, group, requestNum, timeoutMillis, offset, pullOffsetBegin, pullOffsetLast, consumerId, isExclusiveConsume, filters);
        this.consumerAllocationVersion = consumerAllocationVersion;
    }

    public int getConsumerAllocationVersion() {
        return consumerAllocationVersion;
    }
}
