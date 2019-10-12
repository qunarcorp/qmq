package qunar.tc.qmq.consumer.pull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.BrokerService;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public abstract class AbstractPullClient implements PullClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPullClient.class);

    private String subject;
    private String consumerGroup;
    private String partitionName;
    private String brokerGroup;
    private boolean isBroadcast;
    private boolean isOrdered;
    private String consumerId;

    private volatile ConsumeStrategy consumeStrategy;
    private volatile int version;
    private volatile long consumptionExpiredTime;

    public AbstractPullClient(
            String subject,
            String consumerGroup,
            String partitionName,
            String brokerGroup,
            String consumerId,
            ConsumeStrategy consumeStrategy,
            int version,
            boolean isBroadcast,
            boolean isOrdered,
            long consumptionExpiredTime) {
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.partitionName = partitionName;
        this.brokerGroup = brokerGroup;
        this.consumeStrategy = consumeStrategy;
        this.version = version;
        this.consumptionExpiredTime = consumptionExpiredTime;
        this.consumerId = consumerId;
        this.isBroadcast = isBroadcast;
        this.isOrdered = isOrdered;
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
    public String getConsumerId() {
        return consumerId;
    }

    @Override
    public boolean isBroadcast() {
        return isBroadcast;
    }

    @Override
    public boolean isOrdered() {
        return isOrdered;
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
        LOGGER.info("关闭 Consumer {} {} {} {}", getSubject(), getPartitionName(), getBrokerGroup(), getConsumerGroup());
        offline();
        stopPull();
    }
}
