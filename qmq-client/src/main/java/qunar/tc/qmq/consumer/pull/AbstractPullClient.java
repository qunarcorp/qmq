package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.broker.BrokerService;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public abstract class AbstractPullClient implements PullClient {

    private String subject;
    private String consumerGroup;
    private String partitionName;
    private String brokerGroup;
    private BrokerService brokerService;

    private volatile ConsumeStrategy consumeStrategy;
    private volatile int version;
    private volatile long consumptionExpiredTime;

    public AbstractPullClient(String subject, String consumerGroup, String partitionName, String brokerGroup, ConsumeStrategy consumeStrategy, int version, long consumptionExpiredTime, BrokerService brokerService) {
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.partitionName = partitionName;
        this.brokerGroup = brokerGroup;
        this.consumeStrategy = consumeStrategy;
        this.version = version;
        this.consumptionExpiredTime = consumptionExpiredTime;
        this.brokerService = brokerService;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String getBrokerGroup() {
        return brokerGroup;
    }

    @Override
    public long getConsumptionExpiredTime() {
        return consumptionExpiredTime;
    }

    @Override
    public void setConsumptionExpiredTime(long timestamp) {
        this.consumptionExpiredTime = timestamp;
    }

    @Override
    public ConsumeStrategy getConsumeStrategy() {
        return consumeStrategy;
    }

    @Override
    public void setConsumeStrategy(ConsumeStrategy consumeStrategy) {
        this.consumeStrategy = consumeStrategy;
    }

    @Override
    public void destroy() {
        stopPull();
        brokerService.releaseLock(subject, consumerGroup, partitionName, brokerGroup, consumeStrategy);
    }
}
