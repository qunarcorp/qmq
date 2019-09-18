package qunar.tc.qmq.protocol.consumer;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public interface PullRequest {

    String getPartitionName();

    String getGroup();

    int getRequestNum();

    void setRequestNum(int requestNum);

    long getTimeoutMillis();

    long getOffset();

    long getPullOffsetBegin();

    long getPullOffsetLast();

    String getConsumerId();

    boolean isExclusiveConsume();

    List<PullFilter> getFilters();

    void setFilters(List<PullFilter> filters);

    int getConsumerAllocationVersion();
}
