package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

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
    private boolean isBroadcast;
    private boolean isOrdered;
    private String consumerId;
    private final SwitchWaiter onlineSwitcher;

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
            long consumptionExpiredTime,
            BrokerService brokerService,
            MetaInfoService metaInfoService,
            SwitchWaiter onlineSwitcher) {
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.partitionName = partitionName;
        this.brokerGroup = brokerGroup;
        this.consumeStrategy = consumeStrategy;
        this.version = version;
        this.consumptionExpiredTime = consumptionExpiredTime;
        this.brokerService = brokerService;
        this.onlineSwitcher = onlineSwitcher;
        this.consumerId = consumerId;
        this.isBroadcast = isBroadcast;
        this.isOrdered = isOrdered;
        if (this.onlineSwitcher != null) {
            this.onlineSwitcher.addListener(isOnline -> {
                // 上下线主动触发心跳
                MetaInfoRequest request = new MetaInfoRequest(
                        subject,
                        consumerGroup,
                        ClientType.CONSUMER.getCode(),
                        brokerService.getAppCode(),
                        consumerId,
                        ClientRequestType.SWITCH_STATE,
                        isBroadcast,
                        isOrdered
                );
                metaInfoService.sendRequest(request);
            });
        }
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
        stopPull();
        brokerService.releaseLock(subject, consumerGroup, partitionName, brokerGroup, consumeStrategy);
    }

    protected SwitchWaiter getOnlineSwitcher() {
        return onlineSwitcher;
    }
}
