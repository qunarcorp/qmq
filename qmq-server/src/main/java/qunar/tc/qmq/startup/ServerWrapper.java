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

package qunar.tc.qmq.startup;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.consumer.OfflineActionHandler;
import qunar.tc.qmq.consumer.SubscriberStatusChecker;
import qunar.tc.qmq.meta.BrokerRegisterService;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.order.DefaultExclusiveMessageLockManager;
import qunar.tc.qmq.order.ExclusiveMessageLockManager;
import qunar.tc.qmq.processor.*;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.sync.*;
import qunar.tc.qmq.sync.master.MasterSyncNettyServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.constants.BrokerConstants.*;


/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public class ServerWrapper implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerWrapper.class);

    private final DynamicConfig config;
    private final List<Disposable> resources;

    private Integer listenPort;
    private MessageStoreWrapper messageStoreWrapper;
    private Storage storage;
    private SendMessageWorker sendMessageWorker;
    private ConsumerSequenceManager consumerSequenceManager;
    private ExecutorService sendMessageExecutorService;
    private ExecutorService consumeManageExecutorService;

    private SlaveSyncClient slaveSyncClient;
    private MasterSyncNettyServer masterSyncNettyServer;
    private MasterSlaveSyncManager masterSlaveSyncManager;
    private BrokerRegisterService brokerRegisterService;
    private ExclusiveMessageLockManager exclusiveMessageLockManager;

    private SubscriberStatusChecker subscriberStatusChecker;

    private NettyServer nettyServer;

    public ServerWrapper(final DynamicConfig config) {
        this.config = config;
        this.resources = new ArrayList<>();
    }

    public void start(boolean autoOnline) {
        LOG.info("qmq server init started");
        register();
        createStorage();
        startSyncLog();
        initStorage();
        startServeSync();
        startServerHandlers();
        startConsumerChecker();
        addToResources();
        if (autoOnline)
            online();
        LOG.info("qmq server init done");
    }

    public Storage getStorage() {
        return storage;
    }

    public boolean isSlave() {
        // assert BrokerConfig.getBrokerRole() == BrokerRole.STANDBY
        return BrokerConfig.getBrokerRole() == BrokerRole.SLAVE
                || BrokerConfig.getBrokerRole() == BrokerRole.DELAY_SLAVE;
    }

    private void register() {
        this.listenPort = config.getInt(PORT_CONFIG, DEFAULT_PORT);

        final MetaServerLocator metaServerLocator = new MetaServerLocator(config.getString(META_SERVER_ENDPOINT));
        brokerRegisterService = new BrokerRegisterService(listenPort, metaServerLocator);
        brokerRegisterService.start();

        Preconditions.checkState(BrokerConfig.getBrokerRole() != BrokerRole.STANDBY, "目前broker不允许被指定为standby模式");
    }

    private void startConsumerChecker() {
        if (BrokerConfig.getBrokerRole() == BrokerRole.MASTER) {
            subscriberStatusChecker.start();
        }
    }

    private void createStorage() {
        slaveSyncClient = new SlaveSyncClient(config);
        this.storage = new DefaultStorage(BrokerConfig.getBrokerRole(), new StorageConfigImpl(config), new CheckpointLoader() {
            @Override
            public ByteBuf loadCheckpoint() {
                Datagram datagram = null;
                try {
                    datagram = syncCheckpointUntilSuccess();
                    final ByteBuf body = datagram.getBody();
                    body.retain();
                    return body;
                } finally {
                    if (datagram != null) {
                        datagram.release();
                    }
                }
            }

            private Datagram syncCheckpointUntilSuccess() {
                while (true) {
                    try {
                        return slaveSyncClient.syncCheckpoint();
                    } catch (Exception e) {
                        LOG.warn("sync checkpoint failed, will retry after 2 seconds", e);
                        try {
                            TimeUnit.SECONDS.sleep(2);
                        } catch (InterruptedException ignore) {
                            LOG.debug("sync checkpoint interrupted");
                        }
                    }
                }
            }
        });
    }

    private void startSyncLog() {
        if (BrokerConfig.getBrokerRole() == BrokerRole.SLAVE) {
            this.masterSlaveSyncManager = new MasterSlaveSyncManager(slaveSyncClient);
            this.masterSlaveSyncManager.registerProcessor(SyncType.message, new SyncMessageLogProcessor(storage));
            this.masterSlaveSyncManager.registerProcessor(SyncType.action, new SyncActionLogProcessor(storage));
            this.masterSlaveSyncManager.registerProcessor(SyncType.heartbeat, new HeartbeatProcessor(storage));
            this.masterSlaveSyncManager.startSync();
            waitUntilSyncDone();
        }
    }

    private void waitUntilSyncDone() {
        final CheckpointManager checkpointManager = storage.getCheckpointManager();
        final long messageCheckpointOffset = checkpointManager.getMessageCheckpointOffset();
        final long actionCheckpointOffset = checkpointManager.getActionCheckpointOffset();
        while (true) {
            final long maxMessageOffset = storage.getMaxMessageOffset();
            final long maxActionLogOffset = storage.getMaxActionLogOffset();

            if (maxMessageOffset >= messageCheckpointOffset && maxActionLogOffset >= actionCheckpointOffset) {
                return;
            }

            LOG.info("waiting log sync done ...");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException ignore) {
                LOG.debug("sleep interrupted");
            }
        }
    }

    private void initStorage() {
        this.consumerSequenceManager = new ConsumerSequenceManager(storage);
        this.subscriberStatusChecker = new SubscriberStatusChecker(config, storage, consumerSequenceManager);
        this.subscriberStatusChecker.init();
        this.messageStoreWrapper = new MessageStoreWrapper(config, storage, consumerSequenceManager);
        final OfflineActionHandler handler = new OfflineActionHandler(storage);
        this.storage.registerActionEventListener(handler);
        this.storage.start();
        // make sure init this after storage started
        this.consumerSequenceManager.init();

        this.sendMessageExecutorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory("send-message-processor"));
        this.consumeManageExecutorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory("consume-manage-processor"));

        this.sendMessageWorker = new SendMessageWorker(config, messageStoreWrapper);
    }

    private void startServeSync() {
        this.masterSyncNettyServer = new MasterSyncNettyServer(config, storage);
        this.masterSyncNettyServer.registerSyncEvent(sendMessageWorker);
        this.masterSyncNettyServer.start();
    }

    private void startServerHandlers() {
        final ActorSystem actorSystem = new ActorSystem("qmq");
        this.exclusiveMessageLockManager = new DefaultExclusiveMessageLockManager(PartitionConstants.EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS, TimeUnit.MILLISECONDS);
        final PullMessageProcessor pullMessageProcessor = new PullMessageProcessor(config, actorSystem, messageStoreWrapper, subscriberStatusChecker, exclusiveMessageLockManager);
        this.storage.registerEventListener(ConsumerLogWroteEvent.class, pullMessageProcessor);
        final SendMessageProcessor sendMessageProcessor = new SendMessageProcessor(sendMessageWorker);
        final AckMessageProcessor ackMessageProcessor = new AckMessageProcessor(actorSystem, consumerSequenceManager, subscriberStatusChecker);
        final ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(storage);
        final ReleasePullLockProcessor releasePullLockProcessor = new ReleasePullLockProcessor(exclusiveMessageLockManager);

        this.nettyServer = new NettyServer("broker", Runtime.getRuntime().availableProcessors(), listenPort, new BrokerConnectionEventHandler());
        this.nettyServer.registerProcessor(CommandCode.SEND_MESSAGE, sendMessageProcessor, sendMessageExecutorService);
        this.nettyServer.registerProcessor(CommandCode.PULL_MESSAGE, pullMessageProcessor);
        this.nettyServer.registerProcessor(CommandCode.ACK_REQUEST, ackMessageProcessor);
        this.nettyServer.registerProcessor(CommandCode.CONSUME_MANAGE, consumerManageProcessor);
        this.nettyServer.registerProcessor(CommandCode.RELEASE_PULL_LOCK, releasePullLockProcessor);
        this.nettyServer.start();
    }

    private void addToResources() {
        this.resources.add(subscriberStatusChecker);
        this.resources.add(brokerRegisterService);
        this.resources.add(masterSyncNettyServer);
        if (BrokerConfig.getBrokerRole() == BrokerRole.SLAVE) {
            this.resources.add(masterSlaveSyncManager);
        }
        this.resources.add(nettyServer);
        this.resources.add(storage);
    }

    public void online() {
        BrokerConfig.markAsWritable();
        brokerRegisterService.healthSwitch(true);
        subscriberStatusChecker.brokerStatusChanged(true);
    }

    @Override
    public void destroy() {
        offline();
        if (sendMessageExecutorService != null) {
            try {
                sendMessageExecutorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Shutdown sendMessageExecutorService interrupted.");
            }
        }
        if (consumeManageExecutorService != null) {
            try {
                consumeManageExecutorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Shutdown consumeManageExecutorService interrupted.");
            }
        }
        if (resources.isEmpty()) return;

        for (final Disposable resource : resources) {
            try {
                resource.destroy();
            } catch (Throwable e) {
                LOG.error("destroy resource failed", e);
            }
        }
    }

    public void offline() {
        for (int i = 0; i < 3; ++i) {
            try {
                brokerRegisterService.healthSwitch(false);
            } catch (Exception e) {
                LOG.error("offline broker failed.", e);
            }
        }
    }
}
