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
import qunar.tc.qmq.common.*;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.MessageExecutorFactory;
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

    private DefaultMetaInfoService metaInfoService;
    private BrokerService brokerService;
    private PullService pullService;
    private AckService ackService;
    private ExclusiveConsumerLifecycleManager exclusiveConsumerLifecycleManager;

    private String clientId;
    private String appCode;
    private String metaServer;
    private int destroyWaitInSeconds;

    private EnvProvider envProvider;

    public PullRegister(String metaServer) {
        this.metaServer = metaServer;
    }

    public void init() {
        this.metaInfoService = new DefaultMetaInfoService(metaServer);
        this.metaInfoService.setClientId(clientId);
        this.metaInfoService.init();

        this.brokerService = new BrokerServiceImpl(appCode, clientId, metaInfoService);
        OrderStrategyCache.initOrderStrategy(new DefaultMessageGroupResolver(brokerService));
        this.pullService = new PullService(brokerService);
        this.ackService = new AckService(this.brokerService);
        this.exclusiveConsumerLifecycleManager = ClientLifecycleManagerFactory.get();

        this.ackService.setDestroyWaitInSeconds(destroyWaitInSeconds);
        this.ackService.setClientId(clientId);
        this.metaInfoService.setConsumerStateChangedListener(this);
    }

    @Override
    public synchronized Future<PullEntry> registerPullEntry(String subject, String group, RegistParam param) {
        SettableFuture<PullEntry> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(subject, group, ClientType.CONSUMER, appCode);
        metaInfoService.registerResponseSubscriber(new PullEntryUpdater(param, future));
        return future;
    }

    @Override
    public Future<PullConsumer> registerPullConsumer(String subject, String group, boolean isBroadcast) {
        SettableFuture<PullConsumer> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(subject, group, ClientType.CONSUMER, appCode);
        metaInfoService.registerResponseSubscriber(new PullConsumerUpdater(isBroadcast, future));
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
            String group = response.getConsumerGroup();
            PullEntry pullEntry = PullRegister.this.updatePullEntry(subject, group, registParam, response);
            future.set(pullEntry);
            return pullEntry;
        }
    }

    private class PullConsumerUpdater extends PullClientUpdater<PullConsumer> {

        private SettableFuture<PullConsumer> future;
        private boolean isBroadcast;

        public PullConsumerUpdater(boolean isBroadcast, SettableFuture<PullConsumer> future) {
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

    private synchronized PullEntry updatePullEntry(String subject, String group, RegistParam param, ConsumerMetaInfoResponse response) {
        configEnvIsolation(subject, group, param);

        ConsumerAllocation consumerAllocation = response.getConsumerAllocation();
        int version = consumerAllocation.getVersion();
        PullEntry pullEntry = updatePullEntry(subject, group, param, new AlwaysPullStrategy(), response);
        if (RetrySubjectUtils.isDeadRetrySubject(subject)) return pullEntry;
        PullEntry retryPullEntry = updatePullEntry(RetrySubjectUtils.buildRetrySubject(subject, group), group, param, new WeightPullStrategy(), response);

        return new CompositePullEntry<>(subject, group, version, Lists.newArrayList(pullEntry, retryPullEntry));
    }

    private String configEnvIsolation(String subject, String group, RegistParam param) {
        String env;
        if (envProvider != null && !Strings.isNullOrEmpty(env = envProvider.env(subject))) {
            String subEnv = envProvider.subEnv(env);
            final String realGroup = toSubEnvIsolationGroup(group, env, subEnv);
            LOG.info("enable subenv isolation for {}/{}, rename consumer group to {}", subject, group, realGroup);

            param.addFilter(new SubEnvIsolationPullFilter(env, subEnv));
            return realGroup;
        }
        return group;
    }

    private String toSubEnvIsolationGroup(final String originGroup, final String env, final String subEnv) {
        return originGroup + "_" + env + "_" + subEnv;
    }

    private PullEntry updatePullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy, ConsumerMetaInfoResponse response) {
        String subscribeKey = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullEntry oldEntry = pullEntryMap.get(subscribeKey);
        ConsumeMode consumeMode = response.getConsumerAllocation().getConsumeMode();

        if (oldEntry == DefaultPullEntry.EMPTY_PULL_ENTRY) {
            throw new DuplicateListenerException(subscribeKey);
        }

        oldEntry = (PullEntry) createPullClient(subject, group, pullStrategy, response, oldEntry,
                new PullClientFactory<PullEntry>() {
                    @Override
                    public PullEntry createPullClient(String subject, String consumerGroup, String partitionName, int version, PullStrategy pullStrategy) {
                        return createDefaultPullEntry(subject, consumerGroup, partitionName, consumeMode, version, param, pullStrategy);
                    }
                },
                new CompositePullClientFactory<CompositePullEntry, PullEntry>() {
                    @Override
                    public CompositePullEntry createPullClient(String subject, String consumerGroup, int version, List<PullEntry> list) {
                        return new CompositePullEntry(subject, consumerGroup, version, list);
                    }
                });
        pullEntryMap.put(subscribeKey, oldEntry);

        if (isOnline) {
            oldEntry.online(param.getActionSrc());
        } else {
            oldEntry.offline(param.getActionSrc());
        }

        return oldEntry;
    }

    private PullClient createPullClient(
            String subject,
            String consumerGroup,
            PullStrategy pullStrategy,
            ConsumerMetaInfoResponse response,
            PullClient oldClient,
            PullClientFactory pullClientFactory,
            CompositePullClientFactory compositePullClientFactory
    ) {
        ConsumerAllocation consumerAllocation = response.getConsumerAllocation();
        if (oldClient == null) {
            return createNewPullClient(subject, consumerGroup, pullStrategy, consumerAllocation, pullClientFactory, compositePullClientFactory);
        } else {
            return updatePullClient(subject, consumerGroup, pullStrategy, consumerAllocation, pullClientFactory, compositePullClientFactory, (CompositePullClient) oldClient);
        }
    }

    private interface PullClientFactory<T extends PullClient> {
        T createPullClient(String subject, String consumerGroup, String partitionName, int version, PullStrategy pullStrategy);
    }

    private interface CompositePullClientFactory<T extends CompositePullClient, E extends PullClient> {
        T createPullClient(String subject, String consumerGroup, int version, List<E> clientList);
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
        int oldVersion = oldClient.getVersion();
        if (oldVersion < newVersion) {
            // 更新
            List<SubjectLocation> newSubjectLocations = consumerAllocation.getSubjectLocations();
            List<PullClient> oldPullClients = oldClient.getComponents();
            Set<String> newPartitionNames = newSubjectLocations.stream().map(SubjectLocation::getPartitionName).collect(Collectors.toSet());
            Set<String> oldPartitionNames = oldPullClients.stream().map(PullClient::getPartitionName).collect(Collectors.toSet());

            for (PullClient oldPullClient : oldPullClients) {
                String oldPartitionName = oldClient.getPartitionName();
                if (newPartitionNames.contains(oldPartitionName)) {
                    // 获取可复用 entry
                    oldPullClient.setVersion(newVersion);
                    reusePullClients.add(oldPullClient);
                } else {
                    // 获取需要关闭的 entry
                    stalePullClients.add(oldPullClient);
                }
            }

            for (String newPartitionName : newPartitionNames) {
                if (!oldPartitionNames.contains(newPartitionName)) {
                    PullClient newPullClient = pullClientFactory.createPullClient(subject, consumerGroup, newPartitionName, newVersion, pullStrategy);
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
            return compositePullClientFactory.createPullClient(subject, consumerGroup, newVersion, entries);
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
        List<SubjectLocation> subjectLocations = consumerAllocation.getSubjectLocations();
        List<PullClient> clientList = Lists.newArrayList();
        int version = consumerAllocation.getVersion();
        for (SubjectLocation subjectLocation : subjectLocations) {
            String partitionName = subjectLocation.getPartitionName();
            PullClient newDefaultClient = pullClientFactory.createPullClient(subject, consumerGroup, partitionName, version, pullStrategy);
            clientList.add(newDefaultClient);
        }
        return compositePullClientFactory.createPullClient(subject, consumerGroup, version, clientList);
    }

    private PullEntry createDefaultPullEntry(String subject, String consumerGroup, String partitionName, ConsumeMode consumeMode, int allocationVersion, RegistParam param, PullStrategy pullStrategy) {
        ConsumeParam consumeParam = new ConsumeParam(subject, consumerGroup, param);
        ConsumeMessageExecutor consumeMessageExecutor = MessageExecutorFactory.createExecutor(subject, consumerGroup, partitionName, consumeMode, pullExecutor, param.getMessageListener());
        PullEntry pullEntry = new DefaultPullEntry(consumeMessageExecutor, consumeParam, consumerGroup, partitionName, allocationVersion, pullService, ackService, metaInfoService, brokerService, pullStrategy);
        pullEntry.startPull(pullExecutor);
        return pullEntry;
    }

    private synchronized PullConsumer createPullConsumer(String subject, String group, boolean isBroadcast, ConsumerMetaInfoResponse response) {
        String key = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullConsumer oldConsumer = pullConsumerMap.get(key);
        ConsumeMode consumeMode = response.getConsumerAllocation().getConsumeMode();

        PullConsumer pc = (PullConsumer) createPullClient(
                subject,
                group,
                null,
                response,
                oldConsumer,
                new PullClientFactory() {
                    @Override
                    public PullClient createPullClient(String subject, String consumerGroup, String partitionName, int version, PullStrategy pullStrategy) {
                        DefaultPullConsumer pullConsumer = new DefaultPullConsumer(
                                subject,
                                consumerGroup,
                                partitionName,
                                version,
                                isBroadcast,
                                consumeMode,
                                clientId,
                                pullService,
                                ackService,
                                brokerService);
                        pullConsumer.startPull(pullExecutor);
                        return pullConsumer;
                    }
                },
                new CompositePullClientFactory() {
                    @Override
                    public CompositePullClient createPullClient(String subject, String consumerGroup, int version, List clientList) {
                        return new CompositePullEntry(subject, consumerGroup, version, clientList);
                    }
                }
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
