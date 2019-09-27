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

import com.google.common.util.concurrent.SettableFuture;
import qunar.tc.qmq.*;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.metainfoclient.*;
import qunar.tc.qmq.producer.sender.DefaultMessageGroupResolver;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static qunar.tc.qmq.StatusSource.*;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullRegister implements ConsumerRegister, ConsumerStateChangedListener {

    private abstract class PullClientUpdater implements MetaInfoClient.ResponseSubscriber {

        @Override
        public void onSuccess(MetaInfoResponse response) {
            if (response.getClientTypeCode() != ClientType.CONSUMER.getCode()) {
                return;
            }

            updateClient((ConsumerMetaInfoResponse) response);
        }

        abstract void updateClient(ConsumerMetaInfoResponse response);
    }

    private volatile boolean autoOnline = false;
    private PullClientManager<PullEntry> pullEntryManager;
    private PullClientManager<PullConsumer> pullConsumerManager;

    /**
     * 负责执行 partition 的拉取
     */
    private final ExecutorService partitionExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("qmq-pull"));

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
        PullService pullService = new PullService(brokerService);
        SendMessageBack sendMessageBack = new SendMessageBackImpl(brokerService);
        AckService ackService = new DefaultAckService(brokerService, sendMessageBack);

        ackService.setClientId(clientId);
        this.metaInfoService.setConsumerStateChangedListener(this);
        ConsumerOnlineStateManager consumerOnlineStateManager = DefaultConsumerOnlineStateManager.getInstance();

        this.pullEntryManager = new PullEntryManager(
                clientId,
                consumerOnlineStateManager,
                envProvider, pullService,
                ackService,
                brokerService,
                metaInfoService,
                sendMessageBack,
                partitionExecutor
        );

        this.pullConsumerManager = new PullConsumerManager(
                clientId,
                consumerOnlineStateManager,
                pullService,
                ackService,
                brokerService,
                metaInfoService,
                sendMessageBack,
                partitionExecutor
        );
    }

    @Override
    public synchronized Future<PullEntry> registerPullEntry(String subject, String consumerGroup, RegistParam param) {
        SettableFuture<PullEntry> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(appCode, ClientType.CONSUMER.getCode(), subject, consumerGroup, param.isBroadcast(), param.isOrdered(), false);
        metaInfoService.registerResponseSubscriber(new PullClientUpdater() {
            @Override
            void updateClient(ConsumerMetaInfoResponse response) {
                pullEntryManager.updateClient(response, param, autoOnline);
            }
        });
        return future;
    }

    @Override
    public Future<PullConsumer> registerPullConsumer(String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered) {
        SettableFuture<PullConsumer> future = SettableFuture.create();
        metaInfoService.registerHeartbeat(
                appCode,
                ClientType.CONSUMER.getCode(),
                subject,
                consumerGroup,
                isBroadcast,
                isOrdered,
                true
        );
        metaInfoService.registerResponseSubscriber(new PullClientUpdater() {
            @Override
            void updateClient(ConsumerMetaInfoResponse response) {
                pullConsumerManager.updateClient(response, new PullConsumerRegistryParam(isBroadcast, isOrdered, HEALTHCHECKER), autoOnline);
            }
        });
        return future;
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

    private synchronized void changeOnOffline(String subject, String consumerGroup, boolean isOnline, StatusSource statusSource) {
        PullEntry pullEntry = pullEntryManager.getPullClient(subject, consumerGroup);
        PullConsumer pullConsumer = pullConsumerManager.getPullClient(subject, consumerGroup);
        changeOnOffline(pullEntry, isOnline, statusSource);
        changeOnOffline(pullConsumer, isOnline, statusSource);
    }

    private void changeOnOffline(PullClient pullClient, boolean isOnline, StatusSource src) {
        if (pullClient == null) return;

        if (isOnline) {
            pullClient.online(src);
        } else {
            pullClient.offline(src);
        }
    }

    @Override
    public synchronized void setAutoOnline(boolean autoOnline) {
        if (autoOnline) {
            online();
        } else {
            offline();
        }
        this.autoOnline = autoOnline;
    }

    public synchronized boolean offline() {
        this.autoOnline = false;
        for (PullEntry pullEntry : pullEntryManager.getPullClients()) {
            pullEntry.offline(HEALTHCHECKER);
        }
        for (PullConsumer pullConsumer : pullConsumerManager.getPullClients()) {
            pullConsumer.offline(HEALTHCHECKER);
        }
        return true;
    }

    public synchronized boolean online() {
        this.autoOnline = true;
        for (PullEntry pullEntry : pullEntryManager.getPullClients()) {
            pullEntry.online(HEALTHCHECKER);
        }
        for (PullConsumer pullConsumer : pullConsumerManager.getPullClients()) {
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
