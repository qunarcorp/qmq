package qunar.tc.qmq;

import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface PullClient {

    String getSubject();

    String getConsumerGroup();

    String getPartitionName();

    String getBrokerGroup();

    /**
     * 获取 MetaServer 消费的授权时间, 若当前时间超过授权时间, 则不得再消费消息
     */
    long getConsumptionExpiredTime();

    void setConsumptionExpiredTime(long timestamp);

    ConsumeStrategy getConsumeStrategy();

    void setConsumeStrategy(ConsumeStrategy consumeStrategy);

    int getConsumerAllocationVersion();

    void setConsumerAllocationVersion(int version);

    int getPartitionSetVersion();

    void setPartitionSetVersion(int version);

    void startPull(ExecutorService executor);

    void destroy();

    String getConsumerId();

    boolean isBroadcast();

    boolean isOrdered();
}
