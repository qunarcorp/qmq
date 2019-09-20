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

    int getVersion();

    void setVersion(int version);

    void startPull(ExecutorService executor);

    void stopPull();

    void destroy();

    void online(StatusSource statusSource);

    void offline(StatusSource statusSource);
}
