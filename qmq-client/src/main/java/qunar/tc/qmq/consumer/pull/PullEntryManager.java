package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Strings;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.PullEntry;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.consumer.BaseMessageHandler;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutorFactory;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.utils.RetryPartitionUtils;

/**
 * @author zhenwei.liu
 * @since 2019-09-27
 */
public class PullEntryManager extends AbstractPullClientManager<PullEntry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullEntryManager.class);

    private final EnvProvider envProvider;
    private final PullService pullService;
    private final AckService ackService;
    private final BrokerService brokerService;
    private final SendMessageBack sendMessageBack;
    private final ExecutorService partitionExecutor;

    public PullEntryManager(
            String clientId,
            ConsumerOnlineStateManager consumerOnlineStateManager,
            EnvProvider envProvider, PullService pullService,
            AckService ackService,
            BrokerService brokerService,
            SendMessageBack sendMessageBack,
            ExecutorService partitionExecutor) {
        super(clientId, consumerOnlineStateManager);
        this.envProvider = envProvider;
        this.pullService = pullService;
        this.ackService = ackService;
        this.brokerService = brokerService;
        this.sendMessageBack = sendMessageBack;
        this.partitionExecutor = partitionExecutor;
    }

    @Override
    PullEntry doCreatePullClient(
            String subject,
            String consumerGroup,
            String partitionName,
            String brokerGroup,
            ConsumeStrategy consumeStrategy,
            int allocationVersion,
            long consumptionExpiredTime,
            int partitionSetVersion,
            Object registryParam0
    ) {
        RegistParam registryParam = (RegistParam) registryParam0;
        consumerGroup = configEnvIsolation(subject, consumerGroup, registryParam);
        ConsumeParam consumeParam = new ConsumeParam(subject, consumerGroup, registryParam);
        ConsumeMessageExecutor consumeMessageExecutor = ConsumeMessageExecutorFactory.createExecutor(
                consumeStrategy,
                subject,
                consumerGroup,
                partitionName,
                partitionExecutor,
                new BaseMessageHandler(registryParam.getMessageListener()),
                registryParam.getExecutor(),
                consumptionExpiredTime
        );

        boolean isRetry = RetryPartitionUtils.isRetryPartitionName(partitionName);
        PullStrategy pullStrategy = isRetry ? new WeightPullStrategy() : new AlwaysPullStrategy();

        PullEntry pullEntry = new DefaultPullEntry(
                consumeMessageExecutor,
                consumeParam,
                partitionName,
                brokerGroup,
                getClientId(),
                consumeStrategy,
                allocationVersion,
                consumptionExpiredTime,
                pullService,
                ackService,
                brokerService,
                partitionSetVersion,
                pullStrategy,
                sendMessageBack,
                partitionExecutor);
        pullEntry.startPull(partitionExecutor);
        LOGGER.info("创建 pull client {} {} {} {}", subject, partitionName, brokerGroup, consumerGroup);
        return pullEntry;
    }

    @Override
    CompositePullClient doCreateCompositePullClient(String subject, String consumerGroup,
            ConsumeStrategy consumeStrategy, int allocationVersion, long consumptionExpiredTime,
            List<? extends PullClient> clientList, int partitionSetVersion, Object registryParam) {
        RegistParam param = (RegistParam) registryParam;
        return new CompositePullEntry(subject, consumerGroup, getClientId(), consumeStrategy, allocationVersion,
                param.isBroadcast(), param.isOrdered(), partitionSetVersion, consumptionExpiredTime, clientList);

    }

    private String configEnvIsolation(String subject, String consumerGroup, RegistParam param) {
        String env;
        if (envProvider != null && !Strings.isNullOrEmpty(env = envProvider.env(subject))) {
            String subEnv = envProvider.subEnv(env);
            final String realGroup = toSubEnvIsolationGroup(consumerGroup, env, subEnv);
            LOGGER.info("enable subenv isolation for {}/{}, rename consumer consumerGroup to {}", subject,
                    consumerGroup, realGroup);

            param.addFilter(new SubEnvIsolationPullFilter(env, subEnv));
            return realGroup;
        }
        return consumerGroup;
    }

    private String toSubEnvIsolationGroup(final String originGroup, final String env, final String subEnv) {
        return originGroup + "_" + env + "_" + subEnv;
    }
}
