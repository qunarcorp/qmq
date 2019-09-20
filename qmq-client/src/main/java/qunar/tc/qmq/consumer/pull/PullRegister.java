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
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.consumer.BaseMessageHandler;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutorFactory;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.metainfoclient.ConsumerStateChangedListener;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.producer.sender.DefaultMessageGroupResolver;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;

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

    /**
     * 负责执行 partition 的拉取
     */
    private final ExecutorService partitionExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("qmq-pull"));

    private DefaultMetaInfoService metaInfoService;
    private BrokerService brokerService;
    private PullService pullService;
    private AckService ackService;
    private SendMessageBack sendMessageBack;

    private String clientId;
    private String appCode;
    private String metaServer;

    private EnvProvider envProvider;

    public PullRegister() {
    }

    public void init() {
        this.metaInfoService = new DefaultMetaInfoService(metaServer);
        this.metaInfoService.setClientId(clientId);
        this.metaInfoService.init();

        this.brokerService = new BrokerServiceImpl(appCode, clientId, metaInfoService);
        OrderStrategyCache.initOrderStrategy(new DefaultMessageGroupResolver(brokerService));
        this.pullService = new PullService(brokerService);
        this.sendMessageBack = new SendMessageBackImpl(brokerService);
        this.ackService = new DefaultAckService(brokerService, sendMessageBack);

        this.ackService.setClientId(clientId);
        this.metaInfoService.setConsumerStateChangedListener(this);
    }

    @Override
    public synchronized Future<PullEntry> registerPullEntry(String subject, String consumerGroup, RegistParam param) {
        SettableFuture<PullEntry> future = SettableFuture.create();
        MetaInfoRequest request = new MetaInfoRequest(
                subject,
                consumerGroup,
                ClientType.CONSUMER.getCode(),
                appCode,
                clientId,
                ClientRequestType.ONLINE,
                param.isBroadcast(),
                param.isOrdered()
        );
        metaInfoService.request(request);
        metaInfoService.registerResponseSubscriber(new PullEntryUpdater(param, future));
        return future;
    }

    @Override
    public Future<PullConsumer> registerPullConsumer(String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered) {
        SettableFuture<PullConsumer> future = SettableFuture.create();
        MetaInfoRequest request = new MetaInfoRequest(
                subject,
                consumerGroup,
                ClientType.CONSUMER.getCode(),
                appCode,
                clientId,
                ClientRequestType.ONLINE,
                isBroadcast,
                isOrdered
        );
        metaInfoService.request(request);
        metaInfoService.registerResponseSubscriber(new PullConsumerUpdater(isBroadcast, isOrdered, future));
        return future;
    }

    private abstract class PullClientUpdater<T> implements MetaInfoClient.ResponseSubscriber {

        @Override
        public void onResponse(MetaInfoResponse response) {
            if (response.getClientTypeCode() != ClientType.CONSUMER.getCode()) {
                return;
            }

            updateClient((ConsumerMetaInfoResponse) response);
        }

        abstract T updateClient(ConsumerMetaInfoResponse response);
    }

    private class PullEntryUpdater extends PullClientUpdater<PullEntry> {

        private SettableFuture<PullEntry> future;
        private RegistParam registParam;

        public PullEntryUpdater(RegistParam registParam, SettableFuture<PullEntry> future) {
            this.registParam = registParam;
            this.future = future;
        }

        @Override
        public PullEntry updateClient(ConsumerMetaInfoResponse response) {
            String subject = response.getSubject();
            String consumerGroup = response.getConsumerGroup();
            PullEntry pullEntry = PullRegister.this.updatePullEntry(subject, consumerGroup, registParam, response);
            future.set(pullEntry);
            return pullEntry;
        }
    }

    private class PullConsumerUpdater extends PullClientUpdater<PullConsumer> {

        private SettableFuture<PullConsumer> future;
        private boolean isBroadcast;
        private boolean isOrdered;

        public PullConsumerUpdater(boolean isBroadcast, boolean isOrdered, SettableFuture<PullConsumer> future) {
            this.future = future;
            this.isBroadcast = isBroadcast;
            this.isOrdered = isOrdered;
        }

        @Override
        public PullConsumer updateClient(ConsumerMetaInfoResponse response) {
            String subject = response.getSubject();
            String consumerGroup = response.getConsumerGroup();
            PullConsumer pullConsumer = PullRegister.this.createPullConsumer(subject, consumerGroup, isBroadcast, isOrdered, response);
            future.set(pullConsumer);
            return pullConsumer;
        }
    }

    private synchronized PullEntry updatePullEntry(String subject, String consumerGroup, RegistParam param, ConsumerMetaInfoResponse response) {
        configEnvIsolation(subject, consumerGroup, param);

        String entryKey = buildPullClientKey(subject, consumerGroup);
        PullEntry oldEntry = pullEntryMap.get(entryKey);

        ConsumerAllocation consumerAllocation = response.getConsumerAllocation();
        long consumptionExpiredTime = consumerAllocation.getExpired();
        int version = consumerAllocation.getVersion();

        // create retry
        PullEntry pullEntry = updatePullEntry(entryKey, oldEntry, subject, consumerGroup, param, new AlwaysPullStrategy(), consumerAllocation);
        ConsumerAllocation retryConsumerAllocation = consumerAllocation.getRetryConsumerAllocation(consumerGroup);
        PullEntry retryPullEntry = updatePullEntry(entryKey, oldEntry, subject, consumerGroup, param, new WeightPullStrategy(), retryConsumerAllocation);

        PullEntry entry = new CompositePullEntry<>(subject, consumerGroup, version, consumptionExpiredTime, Lists.newArrayList(pullEntry, retryPullEntry), brokerService);

        pullEntryMap.put(entryKey, entry);
        return entry;
    }

    private String configEnvIsolation(String subject, String consumerGroup, RegistParam param) {
        String env;
        if (envProvider != null && !Strings.isNullOrEmpty(env = envProvider.env(subject))) {
            String subEnv = envProvider.subEnv(env);
            final String realGroup = toSubEnvIsolationGroup(consumerGroup, env, subEnv);
            LOG.info("enable subenv isolation for {}/{}, rename consumer consumerGroup to {}", subject, consumerGroup, realGroup);

            param.addFilter(new SubEnvIsolationPullFilter(env, subEnv));
            return realGroup;
        }
        return consumerGroup;
    }

    private String toSubEnvIsolationGroup(final String originGroup, final String env, final String subEnv) {
        return originGroup + "_" + env + "_" + subEnv;
    }

    private PullEntry updatePullEntry(String entryKey, PullEntry oldEntry, String subject, String consumerGroup, RegistParam param, PullStrategy pullStrategy, ConsumerAllocation consumerAllocation) {

        if (oldEntry == DefaultPullEntry.EMPTY_PULL_ENTRY) {
            throw new DuplicateListenerException(entryKey);
        }

        PullEntry updatedEntry = (PullEntry) createPullClient(subject, consumerGroup, pullStrategy, consumerAllocation,
                oldEntry,
                new PullClientFactory<PullEntry>() {
                    @Override
                    public PullEntry createPullClient(String subject, String consumerGroup, String partitionName, String brokerGroup, ConsumeStrategy consumeStrategy, int version, long consumptionExpiredTime, PullStrategy pullStrategy) {
                        return createDefaultPullEntry(subject, consumerGroup, partitionName, brokerGroup, consumeStrategy, version, consumptionExpiredTime, param, pullStrategy);
                    }
                }, new CompositePullClientFactory<CompositePullEntry, PullEntry>() {
                    @Override
                    public CompositePullEntry createPullClient(String subject, String consumerGroup, int version, long consumptionExpiredTime, List<PullEntry> list) {
                        return new CompositePullEntry(subject, consumerGroup, version, consumptionExpiredTime, list, brokerService);
                    }
                });

        if (isOnline) {
            updatedEntry.online(param.getActionSrc());
        } else {
            updatedEntry.offline(param.getActionSrc());
        }

        return updatedEntry;
    }

    private PullClient createPullClient(
            String subject,
            String consumerGroup,
            PullStrategy pullStrategy,
            ConsumerAllocation consumerAllocation,
            PullClient oldClient,
            PullClientFactory pullClientFactory,
            CompositePullClientFactory compositePullClientFactory
    ) {
        if (oldClient == null) {
            return createNewPullClient(subject, consumerGroup, pullStrategy, consumerAllocation, pullClientFactory, compositePullClientFactory);
        } else {
            return updatePullClient(subject, consumerGroup, pullStrategy, consumerAllocation, pullClientFactory, compositePullClientFactory, (CompositePullClient) oldClient);
        }
    }

    private interface PullClientFactory<T extends PullClient> {
        T createPullClient(String subject, String consumerGroup, String partitionName, String brokerGroup, ConsumeStrategy consumeStrategy, int version, long consumptionExpiredTime, PullStrategy pullStrategy);
    }

    private interface CompositePullClientFactory<T extends CompositePullClient, E extends PullClient> {
        T createPullClient(String subject, String consumerGroup, int version, long consumptionExpiredTime, List<E> clientList);
    }

    private PullClient updatePullClient(
            String subject,
            String consumerGroup,
            PullStrategy pullStrategy,
            ConsumerAllocation consumerAllocation,
            PullClientFactory pullClientFactory,
            CompositePullClientFactory compositePullClientFactory,
            CompositePullClient oldClient

    ) {
        List<PullClient> newPullClients = Lists.newArrayList(); // 新增的分区
        List<PullClient> reusePullClients = Lists.newArrayList(); // 可以复用的分区
        List<PullClient> stalePullClients = Lists.newArrayList(); // 需要关闭的分区

        int newVersion = consumerAllocation.getVersion();
        long expired = consumerAllocation.getExpired();
        ConsumeStrategy newConsumeStrategy = consumerAllocation.getConsumeStrategy();
        int oldVersion = oldClient.getVersion();
        if (oldVersion < newVersion) {
            // 更新
            List<PartitionProps> newPartitionProps = consumerAllocation.getPartitionProps();
            List<PullClient> oldPullClients = oldClient.getComponents();
            Set<String> newPartitionNames = newPartitionProps.stream()
                    .map(PartitionProps::getPartitionName)
                    .collect(Collectors.toSet());
            Set<String> oldPartitionNames = oldPullClients.stream().map(PullClient::getPartitionName).collect(Collectors.toSet());

            for (PullClient oldPullClient : oldPullClients) {
                String oldPartitionName = oldClient.getPartitionName();
                if (newPartitionNames.contains(oldPartitionName)) {
                    // 获取可复用 entry
                    oldPullClient.setVersion(newVersion);
                    oldPullClient.setConsumeStrategy(newConsumeStrategy);
                    oldPullClient.setConsumptionExpiredTime(expired);
                    reusePullClients.add(oldPullClient);
                } else {
                    // 获取需要关闭的 entry
                    stalePullClients.add(oldPullClient);
                }
            }

            for (PartitionProps partitionProps : newPartitionProps) {
                String partitionName = partitionProps.getPartitionName();
                String brokerGroup = partitionProps.getBrokerGroup();
                if (!oldPartitionNames.contains(partitionName)) {
                    PullClient newPullClient = pullClientFactory.createPullClient(subject, consumerGroup, partitionName, brokerGroup, newConsumeStrategy, newVersion, expired, pullStrategy);
                    newPullClients.add(newPullClient);
                }
            }

            for (PullClient stalePartitionClient : stalePullClients) {
                // 关闭过期分区
                stalePartitionClient.destroy();
                oldClient.getComponents().remove(stalePartitionClient);
            }

            ArrayList<PullClient> entries = Lists.newArrayList();
            entries.addAll(reusePullClients);
            entries.addAll(newPullClients);
            return compositePullClientFactory.createPullClient(subject, consumerGroup, newVersion, expired, entries);
        } else {
            // 当前版本比 response 中的高
            return oldClient;
        }
    }

    private PullClient createNewPullClient(
            String subject,
            String consumerGroup,
            PullStrategy pullStrategy,
            ConsumerAllocation consumerAllocation,
            PullClientFactory pullClientFactory,
            CompositePullClientFactory compositePullClientFactory
    ) {
        List<PartitionProps> partitionProps = consumerAllocation.getPartitionProps();
        List<PullClient> clientList = Lists.newArrayList();
        ConsumeStrategy consumeStrategy = consumerAllocation.getConsumeStrategy();
        long expired = consumerAllocation.getExpired();
        int version = consumerAllocation.getVersion();
        for (PartitionProps partitionProp : partitionProps) {
            String partitionName = partitionProp.getPartitionName();
            String brokerGroup = partitionProp.getBrokerGroup();
            PullClient newDefaultClient = pullClientFactory.createPullClient(subject, consumerGroup, partitionName, brokerGroup, consumeStrategy, version, expired, pullStrategy);
            clientList.add(newDefaultClient);
        }
        return compositePullClientFactory.createPullClient(subject, consumerGroup, version, expired, clientList);
    }

    private PullEntry createDefaultPullEntry(String subject, String consumerGroup, String partitionName, String brokerGroup, ConsumeStrategy consumeStrategy, int allocationVersion, long consumptionExpiredTime, RegistParam param, PullStrategy pullStrategy) {
        ConsumeParam consumeParam = new ConsumeParam(subject, consumerGroup, param);
        ConsumeMessageExecutor consumeMessageExecutor = ConsumeMessageExecutorFactory.createExecutor(
                consumeStrategy,
                subject,
                consumerGroup,
                partitionName,
                partitionExecutor,
                new BaseMessageHandler(param.getMessageListener()),
                param.getExecutor(),
                consumptionExpiredTime
        );
        PullEntry pullEntry = new DefaultPullEntry(consumeMessageExecutor, consumeParam, partitionName, brokerGroup, consumeStrategy, allocationVersion, consumptionExpiredTime, pullService, ackService, metaInfoService, brokerService, pullStrategy, sendMessageBack);
        pullEntry.startPull(partitionExecutor);
        return pullEntry;
    }

    private synchronized PullConsumer createPullConsumer(String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered, ConsumerMetaInfoResponse response) {
        String key = buildPullClientKey(subject, consumerGroup);
        PullConsumer oldConsumer = pullConsumerMap.get(key);

        PullConsumer pc = (PullConsumer) createPullClient(
                subject,
                consumerGroup,
                new AlwaysPullStrategy(),
                response.getConsumerAllocation(),
                oldConsumer,
                new PullClientFactory() {
                    @Override
                    public PullClient createPullClient(String subject, String consumerGroup, String partitionName, String brokerGroup, ConsumeStrategy consumeStrategy, int version, long consumptionExpiredTime, PullStrategy pullStrategy) {
                        DefaultPullConsumer pullConsumer = new DefaultPullConsumer(
                                subject,
                                consumerGroup,
                                partitionName,
                                brokerGroup,
                                consumeStrategy,
                                version,
                                consumptionExpiredTime,
                                isBroadcast,
                                isOrdered,
                                clientId,
                                pullService,
                                ackService,
                                brokerService,
                                sendMessageBack);
                        pullConsumer.startPull(partitionExecutor);
                        return pullConsumer;
                    }
                },
                new CompositePullClientFactory() {
                    @Override
                    public CompositePullClient createPullClient(String subject, String consumerGroup, int version, long consumptionExpiredTime, List clientList) {
                        return new CompositePullEntry(subject, consumerGroup, version, consumptionExpiredTime, clientList, brokerService);
                    }
                });
        if (pullEntryMap.containsKey(key)) {
            throw new DuplicateListenerException(key);
        }
        pullEntryMap.put(key, DefaultPullEntry.EMPTY_PULL_ENTRY);
        pullConsumerMap.put(key, pc);

        return pc;
    }

    @Override
    public void unregister(String subject, String consumerGroup) {
        changeOnOffline(subject, consumerGroup, false, CODE);
    }

    @Override
    public void online(String subject, String consumerGroup) {
        changeOnOffline(subject, consumerGroup, true, OPS);
    }

    @Override
    public void offline(String subject, String consumerGroup) {
        changeOnOffline(subject, consumerGroup, false, OPS);
    }

    private synchronized void changeOnOffline(String subject, String consumerGroup, boolean isOnline, StatusSource src) {

        final String key = buildPullClientKey(subject, consumerGroup);
        final PullEntry pullEntry = pullEntryMap.get(key);
        changeOnOffline(pullEntry, isOnline, src);

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

        for (PullConsumer pullConsumer : pullConsumerMap.values()) {
            pullConsumer.destroy();
        }
    }

    public PullRegister setMetaServer(String metaServer) {
        this.metaServer = metaServer;
        return this;
    }

    private String buildPullClientKey(String subject, String consumerGroup) {
        return subject + ":" + consumerGroup;
    }
}
