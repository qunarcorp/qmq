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
import qunar.tc.qmq.consumer.MessageExecutor;
import qunar.tc.qmq.consumer.MessageExecutorFactory;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.meta.SubjectLocation;
import qunar.tc.qmq.metainfoclient.ConsumerStateChangedListener;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
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
    private OrderedClientLifecycleManager orderedClientLifecycleManager;

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

        this.brokerService = new BrokerServiceImpl(clientId, metaInfoService);
        this.pullService = new PullService(brokerService);
        this.ackService = new AckService(this.brokerService);
        this.orderedClientLifecycleManager = ClientLifecycleManagerFactory.get();

        this.ackService.setDestroyWaitInSeconds(destroyWaitInSeconds);
        this.ackService.setClientId(clientId);
        this.metaInfoService.setConsumerStateChangedListener(this);

        this.brokerService.setAppCode(appCode);
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

    @Override
    public synchronized Future<PullEntry> registerPullEntry(String subject, String group, RegistParam param) {
        SettableFuture<PullEntry> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(subject, group, ClientType.CONSUMER, appCode);
        metaInfoService.registerResponseSubscriber(new PullEntryUpdater(param, future));
        return future;
    }

    private synchronized PullEntry updatePullEntry(String subject, String group, RegistParam param, ConsumerMetaInfoResponse response) {
        String env;
        String subEnv;
        if (envProvider != null && !Strings.isNullOrEmpty(env = envProvider.env(subject))) {
            subEnv = envProvider.subEnv(env);
            final String realGroup = toSubEnvIsolationGroup(group, env, subEnv);
            LOG.info("enable subenv isolation for {}/{}, rename consumer group to {}", subject, group, realGroup);
            group = realGroup;
            param.addFilter(new SubEnvIsolationPullFilter(env, subEnv));
        }

        ConsumerAllocation consumerAllocation = response.getConsumerAllocation();
        int version = consumerAllocation.getVersion();
        ConsumeMode consumeMode = consumerAllocation.getConsumeMode();

        PullEntry pullEntry = updatePullEntry(subject, group, param, new AlwaysPullStrategy(), response);
        if (RetrySubjectUtils.isDeadRetrySubject(subject)) return pullEntry;
        PullEntry retryPullEntry = updatePullEntry(RetrySubjectUtils.buildRetrySubject(subject, group), group, param, new WeightPullStrategy(), response);

        return new CompositePullEntry<>(subject, group, consumeMode, PartitionConstants.EMPTY_SUBJECT_SUFFIX, version, Lists.newArrayList(pullEntry, retryPullEntry));
    }

    private String toSubEnvIsolationGroup(final String originGroup, final String env, final String subEnv) {
        return originGroup + "_" + env + "_" + subEnv;
    }

    private PullEntry updatePullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy, ConsumerMetaInfoResponse response) {
        String subscribeKey = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullEntry oldEntry = pullEntryMap.get(subscribeKey);

        if (oldEntry == DefaultPullEntry.EMPTY_PULL_ENTRY) {
            throw new DuplicateListenerException(subscribeKey);
        }

        oldEntry = (PullEntry) createPullClient(subject, group, pullStrategy, response, oldEntry,
                (subject1, group1, pullStrategy1, isOrdered) -> createDefaultPullEntry(subject1, group1, param, pullStrategy1, isOrdered),
                );
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
            PullClientFactory pullClientFactory
    ) {

        if (oldClient == null) {
            return createNewPullClient(subject, consumerGroup, pullStrategy, consumerAllocation, pullClientFactory, partitionPullClientFactory, orderedPullClientFactory);
        } else {
            return updatePullClient(subject, consumerGroup, pullStrategy, consumerAllocation, pullClientFactory, partitionPullClientFactory, orderedPullClientFactory, (OrderedPullClient) oldClient);
        }
    }

    private interface PullClientFactory<T extends PullClient> {
        T createPullClient(String subject, String consumerGroup, ConsumeMode consumeMode, String subjectPrefix, int version, PullStrategy pullStrategy);
    }

    private interface CompositePullClientFactory<T extends CompositePullClient> {
        T createPullClient(String subject, String consumerGroup, ConsumeMode consumeMode, String subjectPrefix, int version, List<PullClient> clientList);
    }

    private PullClient updatePullClient(
            String subject,
            String group,
            PullStrategy pullStrategy,
            ConsumerAllocation consumerAllocation,
            PullClientFactory pullClientFactory,
            OrderedPullClient oldClient

    ) {
        ConsumerAllocation oldAllocation = oldClient.getConsumerAllocation();
        List<PartitionPullClient> newPartitionClients = Lists.newArrayList(); // 新增的分区
        List<PartitionPullClient> reusePartitionClients = Lists.newArrayList(); // 可以复用的分区
        List<PartitionPullClient> stalePartitionClients = Lists.newArrayList(); // 需要关闭的分区

        if (oldAllocation.getVersion() < consumerAllocation.getVersion()) {
            // 更新
            Set<Integer> newPartitions = oldClient.getConsumerAllocation().getPhysicalPartitions();
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
                    String orderedSubject = PartitionMessageUtils.getOrderedMessageSubject(subject, newPartitionId);
                    PullClient newDefaultClient = pullClientFactory.createPullClient(orderedSubject, group, pullStrategy, true);
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
            return orderedPullClientFactory.createPullClient(entries, consumerAllocation);
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
        ConsumeMode consumeMode = consumerAllocation.getConsumeMode();
        int version = consumerAllocation.getVersion();
        for (SubjectLocation subjectLocation : subjectLocations) {
            String subjectSuffix = subjectLocation.getSubjectSuffix();

            PullClient newDefaultClient = pullClientFactory.createPullClient(subject, consumerGroup, consumeMode, subjectSuffix, version, pullStrategy);
            clientList.add(newDefaultClient);
        }
        return compositePullClientFactory.createPullClient(subject, consumerGroup, consumeMode, PartitionConstants.EMPTY_SUBJECT_SUFFIX, version, consumerAllocation);
    }

    private PullEntry createDefaultPullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy) {
        ConsumeParam consumeParam = new ConsumeParam(subject, group, param);
        MessageExecutor messageExecutor = MessageExecutorFactory.createExecutor(subject, group, pullExecutor, param.getMessageListener(), isOrdered);
        PullEntry pullEntry = new DefaultPullEntry(messageExecutor, consumeParam, pullService, ackService, metaInfoService, brokerService, pullStrategy);
        pullEntry.startPull(pullExecutor);
        return pullEntry;
    }

    @Override
    public Future<PullConsumer> registerPullConsumer(String subject, String group, boolean isBroadcast) {
        SettableFuture<PullConsumer> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(subject, group, ClientType.CONSUMER, appCode);
        metaInfoService.registerResponseSubscriber(new PullConsumerUpdater(isBroadcast, future));

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
                (subject1, group1, pullStrategy, isOrdered) -> {
                    DefaultPullConsumer pullConsumer = new DefaultPullConsumer(subject1, group1, isBroadcast, clientId, pullService, ackService, brokerService);
                    pullConsumer.startPull(pullExecutor);
                    return pullConsumer;
                },
                (partitionId, pullClient) -> new PartitionPullConsumer(partitionId, (PullConsumer) pullClient),
                (OrderedPullClientFactory<OrderedPullConsumer>) (clients, consumerAllocation) -> new OrderedPullConsumer((List<PartitionPullConsumer>) clients, consumerAllocation)
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
