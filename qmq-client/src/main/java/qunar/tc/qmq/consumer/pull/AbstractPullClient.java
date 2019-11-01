package qunar.tc.qmq.consumer.pull;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullClient;

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
    private volatile int consumerAllocationVersion;
    private volatile int partitionSetVersion;
    private volatile long consumptionExpiredTime;

    public AbstractPullClient(
            String subject,
            String consumerGroup,
            String partitionName,
            String brokerGroup,
            String consumerId,
            ConsumeStrategy consumeStrategy,
            int consumerAllocationVersion,
            boolean isBroadcast,
            boolean isOrdered,
            int partitionSetVersion, long consumptionExpiredTime) {
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.partitionName = partitionName;
        this.brokerGroup = brokerGroup;
        this.consumeStrategy = consumeStrategy;
        this.consumerAllocationVersion = consumerAllocationVersion;
        this.partitionSetVersion = partitionSetVersion;
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
    public int getConsumerAllocationVersion() {
        return consumerAllocationVersion;
    }

    @Override
    public void setConsumerAllocationVersion(int version) {
        this.consumerAllocationVersion = version;
    }

    @Override
    public int getPartitionSetVersion() {
        return partitionSetVersion;
    }

    @Override
    public void setPartitionSetVersion(int version) {
        this.partitionSetVersion = version;
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
    }
}
