/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.*;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.consumer.BufferedMessageExecutor;
import qunar.tc.qmq.consumer.MessageExecutor;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.metainfoclient.ConsumerStateChangedListener;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.producer.OrderedMessageUtils;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static qunar.tc.qmq.StatusSource.*;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullRegister implements ConsumerRegister, ConsumerStateChangedListener {
    private static final Logger LOG = LoggerFactory.getLogger(PullRegister.class);

    private volatile Boolean isOnline = false;

    private final Map<String, PullEntry> pullEntryMap = new HashMap<>();

    private final Map<String, PullConsumer> pullConsumerMap = new HashMap<>();

    private final ExecutorService pullExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("qmq-pull"));

    private final DefaultMetaInfoService metaInfoService;
    private final BrokerService brokerService;
    private final PullService pullService;
    private final AckService ackService;

    private String clientId;
    private String appCode;
    private int destroyWaitInSeconds;

    private EnvProvider envProvider;

    public PullRegister(String metaServer) {
        this.metaInfoService = new DefaultMetaInfoService(metaServer);
        this.brokerService = new BrokerServiceImpl(metaInfoService);
        this.pullService = new PullService();
        this.ackService = new AckService(this.brokerService);
    }

    public void init() {
        this.metaInfoService.setClientId(clientId);
        this.metaInfoService.init();

        this.ackService.setDestroyWaitInSeconds(destroyWaitInSeconds);
        this.ackService.setClientId(clientId);
        this.metaInfoService.setConsumerStateChangedListener(this);

        this.brokerService.setAppCode(appCode);
    }

    private abstract class PullClientCreator<T> implements MetaInfoClient.ResponseSubscriber {

        @Override
        public void onResponse(MetaInfoResponse response) {
            if (response.getClientTypeCode() != ClientType.CONSUMER.getCode()) {
                return;
            }

            updateClient((ConsumerMetaInfoResponse) response);
        }

        abstract T updateClient(ConsumerMetaInfoResponse response);
    }

    private class PullEntryCreator extends PullClientCreator<PullEntry> {

        private SettableFuture<PullEntry> future;
        private RegistParam registParam;

        public PullEntryCreator(RegistParam registParam, SettableFuture<PullEntry> future) {
            this.registParam = registParam;
            this.future = future;
        }

        @Override
        public PullEntry updateClient(ConsumerMetaInfoResponse response) {
            String subject = response.getSubject();
            String group = response.getConsumerGroup();
            PullEntry pullEntry = PullRegister.this.createPullEntry(subject, group, registParam, response);
            future.set(pullEntry);
            return pullEntry;
        }
    }

    private class PullConsumerCreator extends PullClientCreator<PullConsumer> {

        private SettableFuture<PullConsumer> future;
        private boolean isBroadcast;

        public PullConsumerCreator(boolean isBroadcast, SettableFuture<PullConsumer> future) {
            this.future = future;
            this.isBroadcast = isBroadcast;
        }

        @Override
        public PullConsumer updateClient(ConsumerMetaInfoResponse response) {
            String subject = response.getSubject();
            String consumerGroup = response.getConsumerGroup();
            PullConsumer pullConsumer = PullRegister.this.createPullConsumer(subject, consumerGroup, isBroadcast, response);
            future.set(pullConsumer);
            return pullConsumer;
        }
    }

    @Override
    public synchronized Future<PullEntry> registerPullEntry(String subject, String group, RegistParam param) {
        SettableFuture<PullEntry> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(subject, group, ClientType.CONSUMER, appCode);
        metaInfoService.registerResponseSubscriber(new PullEntryCreator(param, future));
        return future;
    }

    private synchronized PullEntry createPullEntry(String subject, String group, RegistParam param, ConsumerMetaInfoResponse response) {
        String env;
        String subEnv;
        if (envProvider != null && !Strings.isNullOrEmpty(env = envProvider.env(subject))) {
            subEnv = envProvider.subEnv(env);
            final String realGroup = toSubEnvIsolationGroup(group, env, subEnv);
            LOG.info("enable subenv isolation for {}/{}, rename consumer group to {}", subject, group, realGroup);
            group = realGroup;
            param.addFilter(new SubEnvIsolationPullFilter(env, subEnv));
        }

        PullEntry pullEntry = registerPullEntry(subject, group, param, new AlwaysPullStrategy(), response);
        if (RetrySubjectUtils.isDeadRetrySubject(subject)) return pullEntry;
        PullEntry retryPullEntry = registerPullEntry(RetrySubjectUtils.buildRetrySubject(subject, group), group, param, new WeightPullStrategy(), response);
        return new CompositePullEntry<>(Lists.newArrayList(pullEntry, retryPullEntry));
    }

    private String toSubEnvIsolationGroup(final String originGroup, final String env, final String subEnv) {
        return originGroup + "_" + env + "_" + subEnv;
    }

    private PullEntry registerPullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy, ConsumerMetaInfoResponse response) {
        String subscribeKey = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullEntry pullEntry = pullEntryMap.get(subscribeKey);

        if (pullEntry == DefaultPullEntry.EMPTY_PULL_ENTRY) {
            throw new DuplicateListenerException(subscribeKey);
        }

        pullEntry = (PullEntry) createPullClient(subject, group, pullStrategy, response, pullEntry,
                (subject1, group1, pullStrategy1) -> createDefaultPullEntry(subject1, group1, param, pullStrategy1),
                (partitionId, pullClient) -> new PartitionPullEntry(partitionId, (PullEntry) pullClient),
                (clients, partitionAllocation) -> new OrderedPullEntry((List<PartitionPullEntry>) clients, partitionAllocation)
        );
        pullEntryMap.put(subscribeKey, pullEntry);

        if (isOnline) {
            pullEntry.online(param.getActionSrc());
        } else {
            pullEntry.offline(param.getActionSrc());
        }

        return pullEntry;
    }

    private PullClient createPullClient(
            String subject,
            String group,
            PullStrategy pullStrategy,
            ConsumerMetaInfoResponse response,
            PullClient oldClient,
            PullClientFactory pullClientFactory,
            PartitionPullClientFactory partitionPullClientFactory,
            OrderedPullClientFactory orderedPullClientFactory
    ) {
        PartitionAllocation partitionAllocation = response.getPartitionAllocation();
        if (partitionAllocation != null) {
            if (oldClient == null) {
                return createNewOrderPullClient(subject, group, pullStrategy, partitionAllocation, pullClientFactory, partitionPullClientFactory, orderedPullClientFactory);
            } else {
                return updateOrderedPullClient(subject, group, pullStrategy, partitionAllocation, pullClientFactory, partitionPullClientFactory, orderedPullClientFactory, (OrderedPullClient) oldClient);
            }
        } else {
            if (oldClient != null) {
                return oldClient;
            }
            return pullClientFactory.createPullClient(subject, group, pullStrategy);
        }
    }

    private interface PullClientFactory<T extends PullClient> {
        T createPullClient(String subject, String group, PullStrategy pullStrategy);
    }

    private interface PartitionPullClientFactory<T extends PartitionPullClient> {
        T createPullClient(int partitionId, PullClient pullClient);
    }

    private interface OrderedPullClientFactory<T extends OrderedPullClient> {
        T createOrderedPullClient(List<? extends PartitionPullClient> clients, PartitionAllocation partitionAllocation);
    }

    private OrderedPullClient updateOrderedPullClient(
            String subject,
            String group,
            PullStrategy pullStrategy,
            PartitionAllocation partitionAllocation,
            PullClientFactory pullClientFactory,
            PartitionPullClientFactory partitionPullClientFactory,
            OrderedPullClientFactory orderedPullClientFactory,
            OrderedPullClient<PartitionPullClient> oldClient

    ) {
        PartitionAllocation oldPartitionAllocation = oldClient.getPartitionAllocation();
        List<PartitionPullClient> newPartitionClients = Lists.newArrayList(); // 新增的分区
        List<PartitionPullClient> reusePartitionClients = Lists.newArrayList(); // 可以复用的分区
        List<PartitionPullClient> stalePartitionClients = Lists.newArrayList(); // 需要关闭的分区

        if (oldPartitionAllocation.getVersion() < partitionAllocation.getVersion()) {
            // 更新
            Set<Integer> newPartitions = oldClient.getPartitionAllocation().getAllocationDetail().getClientId2PhysicalPartitions().get(clientId);
            List<PartitionPullClient> oldPartitionClients = oldClient.getComponents();
            Set<Integer> oldPartitions = oldPartitionClients.stream().map(PartitionPullClient::getPartition).collect(Collectors.toSet());

            for (PartitionPullClient oldPartitionClient : oldPartitionClients) {
                int oldPartition = oldPartitionClient.getPartition();
                if (newPartitions.contains(oldPartition)) {
                    // 获取可复用 entry
                    reusePartitionClients.add(oldPartitionClient);
                } else {
                    // 获取需要关闭的 entry
                    stalePartitionClients.add(oldPartitionClient);
                }
            }

            for (Integer newPartitionId : newPartitions) {
                if (!oldPartitions.contains(newPartitionId)) {
                    String orderedSubject = OrderedMessageUtils.getOrderedMessageSubject(subject, newPartitionId);
                    PullClient newDefaultClient = pullClientFactory.createPullClient(orderedSubject, group, pullStrategy);
                    PartitionPullClient newPartitionClient = partitionPullClientFactory.createPullClient(newPartitionId, newDefaultClient);
                    newPartitionClients.add(newPartitionClient);
                }
            }

            for (PartitionPullClient stalePartitionClient : stalePartitionClients) {
                // 关闭过期分区
                stalePartitionClient.destroy();
                oldClient.getComponents().remove(stalePartitionClient);
            }

            ArrayList<PartitionPullClient> entries = Lists.newArrayList();
            entries.addAll(reusePartitionClients);
            entries.addAll(newPartitionClients);
            return orderedPullClientFactory.createOrderedPullClient(entries, partitionAllocation);
        } else {
            // 当前版本比 response 中的高
            return oldClient;
        }
    }

    private OrderedPullClient createNewOrderPullClient(
            String subject,
            String group,
            PullStrategy pullStrategy,
            PartitionAllocation partitionAllocation,
            PullClientFactory pullClientFactory,
            PartitionPullClientFactory partitionPullClientFactory,
            OrderedPullClientFactory orderedPullClientFactory
    ) {
        Set<Integer> partitionIds = partitionAllocation.getAllocationDetail().getClientId2PhysicalPartitions().get(clientId);
        List<PartitionPullClient> partitionPullClients = Lists.newArrayList();
        for (Integer newPartitionId : partitionIds) {
            String orderedSubject = OrderedMessageUtils.getOrderedMessageSubject(subject, newPartitionId);
            PullClient newDefaultClient = pullClientFactory.createPullClient(orderedSubject, group, pullStrategy);
            PartitionPullClient newPartitionClient = partitionPullClientFactory.createPullClient(newPartitionId, newDefaultClient);
            partitionPullClients.add(newPartitionClient);
        }
        return orderedPullClientFactory.createOrderedPullClient(partitionPullClients, partitionAllocation);
    }

    private PullEntry createDefaultPullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy) {
        ConsumeParam consumeParam = new ConsumeParam(subject, group, param);
        MessageExecutor messageExecutor = new BufferedMessageExecutor(subject, group, param);
        PullEntry pullEntry = new DefaultPullEntry(messageExecutor, consumeParam, pullService, ackService, metaInfoService, brokerService, pullStrategy);
        pullEntry.startPull(pullExecutor);
        return pullEntry;
    }

    @Override
    public Future<PullConsumer> registerPullConsumer(String subject, String group, boolean isBroadcast) {
        SettableFuture<PullConsumer> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(subject, group, ClientType.CONSUMER, appCode);
        metaInfoService.registerResponseSubscriber(new PullConsumerCreator(isBroadcast, future));

        return future;
    }

    private synchronized PullConsumer createPullConsumer(String subject, String group, boolean isBroadcast, ConsumerMetaInfoResponse response) {
        String key = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullConsumer oldConsumer = pullConsumerMap.get(key);

        PullConsumer pc = (PullConsumer) createPullClient(
                subject,
                group,
                null,
                response,
                oldConsumer,
                (subject1, group1, pullStrategy) -> {
                    DefaultPullConsumer pullConsumer = new DefaultPullConsumer(subject1, group1, isBroadcast, clientId, pullService, ackService, brokerService);
                    pullConsumer.startPull(pullExecutor);
                    return pullConsumer;
                },
                (partitionId, pullClient) -> new PartitionPullConsumer(partitionId, (PullConsumer) pullClient),
                (OrderedPullClientFactory<OrderedPullConsumer>) (clients, partitionAllocation) -> new OrderedPullConsumer((List<PartitionPullConsumer>) clients, partitionAllocation)
        );
        if (pullEntryMap.containsKey(key)) {
            throw new DuplicateListenerException(key);
        }
        pullEntryMap.put(key, DefaultPullEntry.EMPTY_PULL_ENTRY);
        pullConsumerMap.put(key, pc);

        return pc;
    }

    @Override
    public void unregister(String subject, String group) {
        changeOnOffline(subject, group, false, CODE);
    }

    @Override
    public void online(String subject, String group) {
        changeOnOffline(subject, group, true, OPS);
    }

    @Override
    public void offline(String subject, String group) {
        changeOnOffline(subject, group, false, OPS);
    }

    private synchronized void changeOnOffline(String subject, String group, boolean isOnline, StatusSource src) {
        final String realSubject = RetrySubjectUtils.getRealSubject(subject);
        final String retrySubject = RetrySubjectUtils.buildRetrySubject(realSubject, group);

        final String key = MapKeyBuilder.buildSubscribeKey(realSubject, group);
        final PullEntry pullEntry = pullEntryMap.get(key);
        changeOnOffline(pullEntry, isOnline, src);

        final PullEntry retryPullEntry = pullEntryMap.get(MapKeyBuilder.buildSubscribeKey(retrySubject, group));
        changeOnOffline(retryPullEntry, isOnline, src);

        PullConsumer pullConsumer = pullConsumerMap.get(key);
        if (pullConsumer == null) return;

        if (isOnline) {
            pullConsumer.online(src);
        } else {
            pullConsumer.offline(src);
        }
    }

    private void changeOnOffline(PullEntry pullEntry, boolean isOnline, StatusSource src) {
        if (pullEntry == null) return;

        if (isOnline) {
            pullEntry.online(src);
        } else {
            pullEntry.offline(src);
        }
    }

    @Override
    public synchronized void setAutoOnline(boolean autoOnline) {
        if (autoOnline) {
            online();
        } else {
            offline();
        }
        isOnline = autoOnline;
    }

    public synchronized boolean offline() {
        isOnline = false;
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.offline(HEALTHCHECKER);
        }
        for (PullConsumer pullConsumer : pullConsumerMap.values()) {
            pullConsumer.offline(HEALTHCHECKER);
        }
        ackService.tryCleanAck();
        return true;
    }

    public synchronized boolean online() {
        isOnline = true;
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.online(HEALTHCHECKER);
        }
        for (PullConsumer pullConsumer : pullConsumerMap.values()) {
            pullConsumer.online(HEALTHCHECKER);
        }
        return true;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setEnvProvider(EnvProvider envProvider) {
        this.envProvider = envProvider;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    @Override
    public synchronized void destroy() {
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.destroy();
        }
        ackService.destroy();
    }

    public void setDestroyWaitInSeconds(int destroyWaitInSeconds) {
        this.destroyWaitInSeconds = destroyWaitInSeconds;
    }
}
