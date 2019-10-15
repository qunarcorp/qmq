package qunar.tc.qmq.consumer.pull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;

/**
 * @author zhenwei.liu
 * @since 2019-09-27
 */
public abstract class AbstractPullClientManager<T extends PullClient> implements PullClientManager<T> {

    private final Map<String, T> clientMap = Maps.newConcurrentMap();
    private final String clientId;
    private final ConsumerOnlineStateManager consumerOnlineStateManager;

    public AbstractPullClientManager(String clientId, ConsumerOnlineStateManager consumerOnlineStateManager) {
        this.clientId = clientId;
        this.consumerOnlineStateManager = consumerOnlineStateManager;
    }

    @Override
    public void updateClient(ConsumerMetaInfoResponse response, Object registryParam) {

        String subject = response.getSubject();
        String consumerGroup = response.getConsumerGroup();

        String clientKey = getClientKey(subject, consumerGroup);

        synchronized (clientKey.intern()) {
            // oldEntry 包含 subject/retry-subject 两个 pullEntry
            // 这两个 PullEntry 又分别包含多个 Partition
            CompositePullClient oldCompositeClient = (CompositePullClient) clientMap.get(clientKey);
            CompositePullClient oldNormalClient =
                    oldCompositeClient == null ? null : (CompositePullClient) oldCompositeClient.getComponents().get(0);
            CompositePullClient oldRetryClient =
                    oldCompositeClient == null ? null : (CompositePullClient) oldCompositeClient.getComponents().get(1);

            ConsumerAllocation consumerAllocation = response.getConsumerAllocation();
            long consumptionExpiredTime = consumerAllocation.getExpired();
            int newAllocationVersion = consumerAllocation.getAllocationVersion();
            int newPartitionSetVersion = consumerAllocation.getPartitionSetVersion();
            ConsumeStrategy consumeStrategy = consumerAllocation.getConsumeStrategy();

            if (oldCompositeClient != null) {
                // 刷新超时时间等信息
                oldCompositeClient.setConsumptionExpiredTime(consumptionExpiredTime);
                oldCompositeClient.setConsumeStrategy(consumeStrategy);

                if (oldCompositeClient.getConsumerAllocationVersion() > newAllocationVersion) {
                    return;
                }

                if (oldCompositeClient.getConsumerAllocationVersion() == newAllocationVersion &&
                        oldCompositeClient.getPartitionSetVersion() >= newPartitionSetVersion) {
                    return;
                }
            }

            CompositePullClient newNormalPullClient = createOrUpdatePullClient(subject, consumerGroup,
                    new AlwaysPullStrategy(), consumerAllocation, oldNormalClient, registryParam);
            ConsumerAllocation retryConsumerAllocation = consumerAllocation.getRetryConsumerAllocation(consumerGroup);
            CompositePullClient newRetryPullClient = createOrUpdatePullClient(subject, consumerGroup,
                    new WeightPullStrategy(), retryConsumerAllocation, oldRetryClient, registryParam);

            ArrayList<CompositePullClient> newComponents = Lists.newArrayList(newNormalPullClient, newRetryPullClient);
            if (oldCompositeClient == null) {
                CompositePullClient newClient = doCreateCompositePullClient(subject, consumerGroup,
                        consumeStrategy, newAllocationVersion, consumptionExpiredTime, newComponents,
                        newPartitionSetVersion,
                        registryParam);
                clientMap.put(clientKey, (T) newClient);
            } else {
                oldCompositeClient.setComponents(newComponents);
                oldCompositeClient.setConsumerAllocationVersion(newAllocationVersion);
                oldCompositeClient.setPartitionSetVersion(newPartitionSetVersion);
            }

            T finalClient = clientMap.get(clientKey);
            SwitchWaiter switchWaiter = consumerOnlineStateManager.getSwitchWaiter(subject, consumerGroup);
            if (switchWaiter.isOnline()) {
                finalClient.online();
            }
        }
    }

    private CompositePullClient createOrUpdatePullClient(
            String subject,
            String consumerGroup,
            PullStrategy pullStrategy,
            ConsumerAllocation consumerAllocation,
            CompositePullClient oldClient,
            Object registryParam
    ) {
        if (oldClient == null) {
            return createNewPullClient(subject, consumerGroup, pullStrategy, consumerAllocation, registryParam);
        } else {
            return updatePullClient(subject, consumerGroup, pullStrategy, consumerAllocation, oldClient, registryParam);
        }
    }

    private CompositePullClient createNewPullClient(
            String subject,
            String consumerGroup,
            PullStrategy pullStrategy,
            ConsumerAllocation consumerAllocation,
            Object registryParam
    ) {
        Collection<PartitionProps> partitionProps = consumerAllocation.getPartitionProps();
        List<PullClient> clientList = Lists.newArrayList();
        ConsumeStrategy consumeStrategy = consumerAllocation.getConsumeStrategy();
        long expired = consumerAllocation.getExpired();
        int allocationVersion = consumerAllocation.getAllocationVersion();
        int partitionSetVersion = consumerAllocation.getPartitionSetVersion();
        for (PartitionProps partitionProp : partitionProps) {
            String partitionName = partitionProp.getPartitionName();
            String brokerGroup = partitionProp.getBrokerGroup();
            PullClient newDefaultClient = doCreatePullClient(subject, consumerGroup, partitionName, brokerGroup,
                    consumeStrategy, allocationVersion, expired, pullStrategy, partitionSetVersion, registryParam);
            clientList.add(newDefaultClient);
        }
        return doCreateCompositePullClient(subject, consumerGroup, consumeStrategy, allocationVersion, expired,
                clientList,
                partitionSetVersion, registryParam);
    }

    private CompositePullClient updatePullClient(
            String subject,
            String consumerGroup,
            PullStrategy pullStrategy,
            ConsumerAllocation consumerAllocation,
            CompositePullClient oldClient,
            Object registryParam

    ) {
        List<T> newPullClients = Lists.newArrayList(); // 新增的分区
        List<T> reusePullClients = Lists.newArrayList(); // 可以复用的分区
        List<T> stalePullClients = Lists.newArrayList(); // 需要关闭的分区

        int newConsumerAllocationVersion = consumerAllocation.getAllocationVersion();
        long expired = consumerAllocation.getExpired();
        ConsumeStrategy newConsumeStrategy = consumerAllocation.getConsumeStrategy();
        int newPartitionSetVersion = consumerAllocation.getPartitionSetVersion();
        // 更新
        Collection<PartitionProps> newPartitionProps = consumerAllocation.getPartitionProps();
        List<T> oldPullClients = oldClient.getComponents();
        Set<String> newPartitionKeys = newPartitionProps.stream()
                .map(this::createPartitionKey)
                .collect(Collectors.toSet());
        Set<String> oldPartitionKeys = oldPullClients.stream().map(this::createPartitionKey)
                .collect(Collectors.toSet());

        for (T oldPullClient : oldPullClients) {
            String oldPartitionKey = createPartitionKey(oldPullClient);
            if (newPartitionKeys.contains(oldPartitionKey)) {
                // 获取可复用 entry
                oldPullClient.setConsumerAllocationVersion(newConsumerAllocationVersion);
                oldPullClient.setPartitionSetVersion(newPartitionSetVersion);
                oldPullClient.setConsumeStrategy(newConsumeStrategy);
                oldPullClient.setConsumptionExpiredTime(expired);
                reusePullClients.add(oldPullClient);
            } else {
                // 获取需要关闭的 entry
                stalePullClients.add(oldPullClient);
            }
        }

        for (PartitionProps partitionProps : newPartitionProps) {
            String partitionKey = createPartitionKey(partitionProps);
            if (!oldPartitionKeys.contains(partitionKey)) {
                String partitionName = partitionProps.getPartitionName();
                String brokerGroup = partitionProps.getBrokerGroup();
                T newPullClient = doCreatePullClient(subject, consumerGroup, partitionName, brokerGroup,
                        newConsumeStrategy, newConsumerAllocationVersion, expired, pullStrategy, newPartitionSetVersion,
                        registryParam);
                newPullClients.add(newPullClient);
            }
        }

        for (PullClient stalePartitionClient : stalePullClients) {
            // 关闭过期分区
            stalePartitionClient.destroy();
            oldClient.getComponents().remove(stalePartitionClient);
        }

        ArrayList<T> entries = Lists.newArrayList();
        entries.addAll(reusePullClients);
        entries.addAll(newPullClients);
        return doCreateCompositePullClient(subject, consumerGroup, newConsumeStrategy, newConsumerAllocationVersion,
                expired, entries,
                newPartitionSetVersion, registryParam);
    }

    abstract T doCreatePullClient(String subject, String consumerGroup, String partitionName, String brokerGroup,
            ConsumeStrategy consumeStrategy, int allocationVersion, long consumptionExpiredTime,
            PullStrategy pullStrategy,
            int partitionSetVersion, Object registryParam);

    abstract CompositePullClient doCreateCompositePullClient(String subject, String consumerGroup,
            ConsumeStrategy consumeStrategy, int allocationVersion, long consumptionExpiredTime,
            List<? extends PullClient> clientList, int partitionSetVersion, Object registryParam);

    abstract StatusSource getStatusSource(Object registryParam);

    private String createPartitionKey(PartitionProps pp) {
        return createPartitionKey(pp.getBrokerGroup(), pp.getPartitionName());
    }

    private String createPartitionKey(PullClient pc) {
        return createPartitionKey(pc.getBrokerGroup(), pc.getPartitionName());
    }

    private String createPartitionKey(String brokerGroup, String partitionName) {
        return brokerGroup + ":" + partitionName;
    }

    @Override
    public T getPullClient(String subject, String consumerGroup) {
        String key = getClientKey(subject, consumerGroup);
        return clientMap.get(key);
    }

    @Override
    public Collection<T> getPullClients() {
        return clientMap.values();
    }

    private String getClientKey(String subject, String consumerGroup) {
        return subject + ":" + consumerGroup;
    }

    public String getClientId() {
        return clientId;
    }

    public ConsumerOnlineStateManager getConsumerOnlineStateManager() {
        return consumerOnlineStateManager;
    }
}
