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

package qunar.tc.qmq.delay.startup;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.delay.DefaultDelayLogFacade;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.ScheduleIndex;
import qunar.tc.qmq.delay.config.DefaultStoreConfiguration;
import qunar.tc.qmq.delay.meta.BrokerRoleManager;
import qunar.tc.qmq.delay.receiver.ReceivedDelayMessageProcessor;
import qunar.tc.qmq.delay.receiver.Receiver;
import qunar.tc.qmq.delay.sender.NettySender;
import qunar.tc.qmq.delay.sender.Sender;
import qunar.tc.qmq.delay.sync.master.MasterSyncNettyServer;
import qunar.tc.qmq.delay.sync.slave.SlaveSynchronizer;
import qunar.tc.qmq.delay.wheel.WheelTickManager;
import qunar.tc.qmq.meta.BrokerRegisterService;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.netty.DefaultConnectionEventHandler;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.utils.NetworkUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-27 17:05
 */
public class ServerWrapper implements Disposable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerWrapper.class);

    private static final String META_SERVER_ENDPOINT = "meta.server.endpoint";
    private static final String APP_CODE_KEY = "app.code";
    private static final String PORT_CONFIG = "broker.port";

    private static final Integer DEFAULT_PORT = 20801;

    private ExecutorService receiveMessageExecutorService;
    private BrokerRegisterService brokerRegisterService;
    private ReceivedDelayMessageProcessor processor;
    private MasterSyncNettyServer syncNettyServer;
    private SlaveSynchronizer slaveSynchronizer;
    private NettyServer nettyServer;
    private DelayLogFacade facade;
    private WheelTickManager wheelTickManager;
    private Integer listenPort;
    private Receiver receiver;
    private DynamicConfig config;
    private DefaultStoreConfiguration storeConfig;

    public void start(boolean autoOnline) {
        init();
        register();
        startServer();
        sync();
        if (autoOnline)
            online();
    }

    public void online() {
        BrokerConfig.markAsWritable();
        brokerRegisterService.healthSwitch(true);
    }

    // TODO(zhenwei.liu) delay 是不是应该使用 qmq-client 来发消息?
    private void init() {
        this.config = DynamicConfigLoader.load("delay.properties");
        this.listenPort = config.getInt(PORT_CONFIG, DEFAULT_PORT);

        this.receiveMessageExecutorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory("send-message-processor"));

        final Sender sender = new NettySender();
        storeConfig = new DefaultStoreConfiguration(config);
        this.facade = new DefaultDelayLogFacade(storeConfig, this::iterateCallback);

        String appCode = config.getString(APP_CODE_KEY, "delay-server");
        String metaServerAddress = config.getString(META_SERVER_ENDPOINT);
        DefaultMetaInfoService metaInfoService = new DefaultMetaInfoService(metaServerAddress);
        metaInfoService.init();
        BrokerService brokerService = new BrokerServiceImpl(appCode, "delay-" + NetworkUtils.getLocalAddress(), metaInfoService);
        this.wheelTickManager = new WheelTickManager(storeConfig, brokerService, facade, sender);

        this.receiver = new Receiver(config, facade);

        final MetaServerLocator metaServerLocator = new MetaServerLocator(metaServerAddress);
        this.brokerRegisterService = new BrokerRegisterService(listenPort, metaServerLocator);

        this.processor = new ReceivedDelayMessageProcessor(receiver);
    }

    private boolean iterateCallback(final ScheduleIndex index) {
        long scheduleTime = index.getScheduleTime();
        long offset = index.getOffset();
        if (wheelTickManager.canAdd(scheduleTime, offset)) {
            wheelTickManager.addWHeel(index);
            return true;
        }

        return false;
    }

    private void register() {
        this.brokerRegisterService.start();

        Preconditions.checkState(BrokerConfig.getBrokerRole() != BrokerRole.STANDBY, "目前broker不允许被指定为standby模式");
    }

    private void sync() {
        this.syncNettyServer = new MasterSyncNettyServer(storeConfig.getSegmentScale(), config, facade);
        this.syncNettyServer.registerSyncEvent(receiver);
        this.syncNettyServer.start();

        if (BrokerRoleManager.isDelaySlave()) {
            this.slaveSynchronizer = new SlaveSynchronizer(BrokerConfig.getMasterAddress(), config, facade);
            this.slaveSynchronizer.startSync();
        }
    }

    private void startServer() {
        wheelTickManager.start();
        facade.start();
        facade.blockUntilReplayDone();
        startNettyServer();
    }

    private void startNettyServer() {
        this.nettyServer = new NettyServer("delay-broker", Runtime.getRuntime().availableProcessors(), listenPort, new DefaultConnectionEventHandler("delay-broker"));
        this.nettyServer.registerProcessor(CommandCode.SEND_MESSAGE, processor, receiveMessageExecutorService);
        this.nettyServer.start();
    }

    @Override
    public void destroy() {
        offline();
        receiveMessageExecutorService.shutdown();
        try {
            receiveMessageExecutorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Shutdown receiveMessageExecutorService interrupted.");
        }
        brokerRegisterService.destroy();
        syncNettyServer.destroy();
        if (BrokerRoleManager.isDelaySlave()) {
            slaveSynchronizer.destroy();
        }
        nettyServer.destroy();
        facade.shutdown();
        wheelTickManager.shutdown();
    }

    public void offline() {
        for (int i = 0; i < 3; ++i) {
            try {
                brokerRegisterService.healthSwitch(false);
            } catch (Exception e) {
                LOGGER.error("offline delay server failed", e);
            }
        }
    }
}
