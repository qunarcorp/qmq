package qunar.tc.qmq.consumer.pull;

import java.util.List;
import java.util.concurrent.ExecutorService;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;

/**
 * @author zhenwei.liu
 * @since 2019-09-27
 */
public class PullConsumerManager extends AbstractPullClientManager<PullConsumer> {

    private PullService pullService;
    private AckService ackService;
    private BrokerService brokerService;
    private SendMessageBack sendMessageBack;
    private ExecutorService partitionExecutor;

    public PullConsumerManager(
            String clientId,
            ConsumerOnlineStateManager consumerOnlineStateManager,
            PullService pullService,
            AckService ackService,
            BrokerService brokerService,
            SendMessageBack sendMessageBack,
            ExecutorService partitionExecutor) {
        super(clientId, consumerOnlineStateManager);
        this.pullService = pullService;
        this.ackService = ackService;
        this.brokerService = brokerService;
        this.sendMessageBack = sendMessageBack;
        this.partitionExecutor = partitionExecutor;
    }

    @Override
    PullConsumer doCreatePullClient(String subject, String consumerGroup, String partitionName, String brokerGroup,
            ConsumeStrategy consumeStrategy, int allocationVersion, long consumptionExpiredTime,
            int partitionSetVersion, Object param) {
        PullConsumerRegistryParam registryParam = (PullConsumerRegistryParam) param;
        DefaultPullConsumer pullConsumer = new DefaultPullConsumer(
                subject,
                consumerGroup,
                partitionName,
                brokerGroup,
                getClientId(),
                consumeStrategy,
                allocationVersion,
                consumptionExpiredTime,
                registryParam.isBroadcast(),
                registryParam.isOrdered(),
                pullService,
                ackService,
                brokerService,
                sendMessageBack,
                partitionSetVersion,
                getConsumerOnlineStateManager());
        pullConsumer.startPull(partitionExecutor);
        return pullConsumer;
    }

    @Override
    CompositePullClient doCreateCompositePullClient(String subject, String consumerGroup,
            ConsumeStrategy consumeStrategy, int allocationVersion, long consumptionExpiredTime,
            List<? extends PullClient> clientList, int partitionSetVersion, Object param) {
        PullConsumerRegistryParam registryParam = (PullConsumerRegistryParam) param;
        return new CompositePullConsumer<>(
                subject,
                consumerGroup,
                getClientId(),
                consumeStrategy,
                allocationVersion,
                registryParam.isBroadcast(),
                registryParam.isOrdered(),
                partitionSetVersion,
                consumptionExpiredTime,
                (List<? extends PullConsumer>) clientList,
                getConsumerOnlineStateManager()
        );
    }
}
