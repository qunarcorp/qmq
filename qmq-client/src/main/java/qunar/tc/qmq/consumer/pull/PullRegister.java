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

import static qunar.tc.qmq.StatusSource.CODE;
import static qunar.tc.qmq.StatusSource.HEALTHCHECKER;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.PullEntry;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.broker.impl.SwitchWaiter;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.metainfoclient.DefaultConsumerOnlineStateManager;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.producer.sender.DefaultMessageGroupResolver;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullRegister implements ConsumerRegister {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullRegister.class);

    private abstract class PullClientUpdater implements MetaInfoClient.ResponseSubscriber {

        private String subject;
        private String consumerGroup;

        public PullClientUpdater(String subject, String consumerGroup) {
            this.subject = subject;
            this.consumerGroup = consumerGroup;
        }

        @Override
        public void onSuccess(MetaInfoResponse response) {
            if (response.getClientTypeCode() != ClientType.CONSUMER.getCode()) {
                return;
            }

            String respSubject = response.getSubject();
            String respConsumerGroup = response.getConsumerGroup();

            if (Objects.equals(respSubject, subject) || Objects.equals(respConsumerGroup, consumerGroup)) {
                updateClient((ConsumerMetaInfoResponse) response);
            }
        }

        abstract void updateClient(ConsumerMetaInfoResponse response);
    }

    private PullClientManager<PullEntry> pullEntryManager;
    private PullClientManager<PullConsumer> pullConsumerManager;

    /**
     * 负责执行 partition 的拉取
     */
    private final ExecutorService partitionExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("qmq-pull"));
    private final Object token = new Object();
    private final ConcurrentMap<String, Object> registerHistory = Maps.newConcurrentMap();

    private ConsumerOnlineStateManager consumerOnlineStateManager;
    private DefaultMetaInfoService metaInfoService;

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

        BrokerService brokerService = new BrokerServiceImpl(appCode, clientId, metaInfoService);
        OrderStrategyCache.initOrderStrategy(new DefaultMessageGroupResolver(brokerService));
        PullService pullService = new PullService();
        SendMessageBack sendMessageBack = new SendMessageBackImpl(brokerService);
        AckService ackService = new DefaultAckService(brokerService, sendMessageBack);

        ackService.setClientId(clientId);
        this.consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();

        this.pullEntryManager = new PullEntryManager(
                clientId,
                consumerOnlineStateManager,
                envProvider, pullService,
                ackService,
                brokerService,
                sendMessageBack,
                partitionExecutor
        );

        this.pullConsumerManager = new PullConsumerManager(
                clientId,
                consumerOnlineStateManager,
                pullService,
                ackService,
                brokerService,
                sendMessageBack,
                partitionExecutor
        );
    }

    @Override
    public Future<PullEntry> registerPullEntry(String subject, String consumerGroup, RegistParam param) {
        checkDuplicatedConsumer(subject, consumerGroup);
        SettableFuture<PullEntry> future = SettableFuture.create();
        boolean isOrdered = param.isOrdered();
        boolean isBroadcast = param.isBroadcast();
        consumerOnlineStateManager.registerConsumer(subject, consumerGroup);
        consumerOnlineStateManager.addOnlineStateListener(subject, consumerGroup, (isOnline) -> {
            final PullEntry pullClient = pullEntryManager.getPullClient(subject, consumerGroup);
            if (pullClient == null) {
                return;
            }
            if (isOnline) {
                LOGGER.info("consumer offline, subject {} consumerGroup {} broadcast {} ordered {} clientId {}",
                        subject, consumerGroup, isBroadcast, isOrdered, clientId);
            } else {
                // 触发 Consumer 下线清理操作
                LOGGER.info("consumer offline, Subject {} ConsumerGroup {} Broadcast {} ordered {} clientId {}",
                        subject, consumerGroup, isBroadcast, isOrdered, clientId);
                pullClient.offline();
            }

            // 上下线主动触发心跳
            MetaInfoRequest request = new MetaInfoRequest(
                    subject,
                    consumerGroup,
                    ClientType.CONSUMER.getCode(),
                    appCode,
                    clientId,
                    ClientRequestType.SWITCH_STATE,
                    isBroadcast,
                    isOrdered
            );
            request.setConsumeStrategy(pullClient.getConsumeStrategy());
            metaInfoService.sendRequest(request);
        });
        metaInfoService.registerHeartbeat(appCode, ClientType.CONSUMER.getCode(), subject, consumerGroup,
                isBroadcast,
                isOrdered);
        metaInfoService.registerResponseSubscriber(new PullClientUpdater(subject, consumerGroup) {

            @Override
            void updateClient(ConsumerMetaInfoResponse response) {
                pullEntryManager.updateClient(response, param);
                final PullEntry pullClient = pullEntryManager.getPullClient(subject, consumerGroup);
                future.set(pullClient);
            }
        });
        return future;
    }

    @Override
    public Future<PullConsumer> registerPullConsumer(String subject, String consumerGroup, boolean isBroadcast,
            boolean isOrdered) {
        checkDuplicatedConsumer(subject, consumerGroup);
        SettableFuture<PullConsumer> future = SettableFuture.create();
        consumerOnlineStateManager.registerConsumer(subject, consumerGroup);
        consumerOnlineStateManager.addOnlineStateListener(subject, consumerGroup, (isOnline) -> {
            PullConsumer pullClient = pullConsumerManager.getPullClient(subject, consumerGroup);
            if (pullClient == null) {
                return;
            }
            if (isOnline) {
                LOGGER.info("consumer offline, subject {} consumerGroup {} broadcast {} ordered {} clientId {}",
                        subject, consumerGroup, isBroadcast, isOrdered, clientId);
            } else {
                // 触发 Consumer 下线清理操作
                LOGGER.info("consumer offline, Subject {} ConsumerGroup {} Broadcast {} ordered {} clientId {}",
                        subject, consumerGroup, isBroadcast, isOrdered, clientId);
                pullClient.offline();
            }

            // 上下线主动触发心跳
            MetaInfoRequest request = new MetaInfoRequest(
                    subject,
                    consumerGroup,
                    ClientType.CONSUMER.getCode(),
                    appCode,
                    clientId,
                    ClientRequestType.SWITCH_STATE,
                    isBroadcast,
                    isOrdered
            );
            request.setConsumeStrategy(pullClient.getConsumeStrategy());
            metaInfoService.sendRequest(request);
        });
        metaInfoService.registerHeartbeat(
                appCode,
                ClientType.CONSUMER.getCode(),
                subject,
                consumerGroup,
                isBroadcast,
                isOrdered
        );
        metaInfoService.registerResponseSubscriber(new PullClientUpdater(subject, consumerGroup) {
            @Override
            void updateClient(ConsumerMetaInfoResponse response) {
                pullConsumerManager
                        .updateClient(response, new PullConsumerRegistryParam(isBroadcast, isOrdered, HEALTHCHECKER));
                future.set(pullConsumerManager.getPullClient(subject, consumerGroup));
            }
        });
        return future;
    }

    private void checkDuplicatedConsumer(String subject, String consumerGroup) {
        String consumerKey = getConsumerKey(subject, consumerGroup);
        Object old = registerHistory.putIfAbsent(consumerKey, token);
        if (old != null) {
            throw new DuplicateListenerException(consumerKey);
        }
    }

    private String getConsumerKey(String subject, String consumerGroup) {
        return subject + ":" + consumerGroup;
    }

    @Override
    public void unregister(String subject, String consumerGroup) {
        SwitchWaiter switchWaiter = consumerOnlineStateManager.getSwitchWaiter(subject, consumerGroup);
        switchWaiter.off(CODE);
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
        consumerOnlineStateManager.offlineHealthCheck();
        for (PullEntry pullEntry : pullEntryManager.getPullClients()) {
            pullEntry.destroy();
        }

        for (PullConsumer pullConsumer : pullConsumerManager.getPullClients()) {
            pullConsumer.destroy();
        }
    }

    public PullRegister setMetaServer(String metaServer) {
        this.metaServer = metaServer;
        return this;
    }
}