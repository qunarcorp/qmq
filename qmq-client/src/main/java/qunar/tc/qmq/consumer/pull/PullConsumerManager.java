package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.*;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;

import java.util.List;
import java.util.concurrent.ExecutorService;

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
    PullConsumer doCreatePullClient(String subject, String consumerGroup, String partitionName, String brokerGroup, ConsumeStrategy consumeStrategy, int version, long consumptionExpiredTime, PullStrategy pullStrategy, Object param) {
        PullConsumerRegistryParam registryParam = (PullConsumerRegistryParam) param;
        DefaultPullConsumer pullConsumer = new DefaultPullConsumer(
                subject,
                consumerGroup,
                partitionName,
                brokerGroup,
                clientId,
                consumeStrategy,
                version,
                consumptionExpiredTime,
                registryParam.isBroadcast(),
                registryParam.isOrdered(),
                pullService,
                ackService,
                brokerService,
                sendMessageBack,
                consumerOnlineStateManager);
        pullConsumer.startPull(partitionExecutor);
        return pullConsumer;
    }

    @Override
    CompositePullClient doCreateCompositePullClient(String subject, String consumerGroup, int version, long consumptionExpiredTime, List<? extends PullClient> clientList, Object param) {
        PullConsumerRegistryParam registryParam = (PullConsumerRegistryParam) param;
        return new CompositePullConsumer<>(
                subject,
                consumerGroup,
                clientId,
                version,
                registryParam.isBroadcast(),
                registryParam.isOrdered(),
                consumptionExpiredTime,
                (List<? extends PullConsumer>) clientList,
                consumerOnlineStateManager
        );
    }
}
