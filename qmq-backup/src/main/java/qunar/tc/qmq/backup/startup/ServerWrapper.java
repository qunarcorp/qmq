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

package qunar.tc.qmq.backup.startup;

import static qunar.tc.qmq.backup.service.DicService.SIX_DIGIT_FORMAT_PATTERN;
import static qunar.tc.qmq.constants.BrokerConstants.DEFAULT_PORT;
import static qunar.tc.qmq.constants.BrokerConstants.META_SERVER_ENDPOINT;
import static qunar.tc.qmq.constants.BrokerConstants.PORT_CONFIG;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.ActionRecord;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.config.DefaultBackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.BatchBackup;
import qunar.tc.qmq.backup.service.BatchBackupManager;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.service.ScheduleFlushManager;
import qunar.tc.qmq.backup.service.SyncLogIterator;
import qunar.tc.qmq.backup.service.impl.ActionSyncLogIterator;
import qunar.tc.qmq.backup.service.impl.DbDicService;
import qunar.tc.qmq.backup.service.impl.DeadMessageBatchBackup;
import qunar.tc.qmq.backup.service.impl.DeadMessageContentBatchBackup;
import qunar.tc.qmq.backup.service.impl.DeadMsgEventBusListener;
import qunar.tc.qmq.backup.service.impl.DeadRecordBatchBackup;
import qunar.tc.qmq.backup.service.impl.IndexEventBusListener;
import qunar.tc.qmq.backup.service.impl.IndexFileStore;
import qunar.tc.qmq.backup.service.impl.MessageIndexBatchBackup;
import qunar.tc.qmq.backup.service.impl.MessageServiceImpl;
import qunar.tc.qmq.backup.service.impl.RecordBatchBackup;
import qunar.tc.qmq.backup.store.DicStore;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.store.RecordStore;
import qunar.tc.qmq.backup.store.RocksDBStore;
import qunar.tc.qmq.backup.store.impl.DbDicDao;
import qunar.tc.qmq.backup.store.impl.FactoryStoreImpl;
import qunar.tc.qmq.backup.store.impl.RocksDBStoreImpl;
import qunar.tc.qmq.backup.sync.BackupActionLogSyncProcessor;
import qunar.tc.qmq.backup.sync.BackupMessageLogSyncProcessor;
import qunar.tc.qmq.backup.sync.HeartBeatProcessor;
import qunar.tc.qmq.backup.sync.IndexLogSyncDispatcher;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.meta.BrokerRegisterService;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.CheckpointManager;
import qunar.tc.qmq.store.IndexLog;
import qunar.tc.qmq.store.LogIterateService;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.store.StorageConfig;
import qunar.tc.qmq.store.StorageConfigImpl;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.sync.MasterSlaveSyncManager;
import qunar.tc.qmq.sync.SlaveSyncClient;
import qunar.tc.qmq.sync.SyncType;
import qunar.tc.qmq.utils.NetworkUtils;

public class ServerWrapper implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerWrapper.class);

    private final BackupConfig config;
    private final BackupConfig deadConfig;
    private final DicService dicService;
    private final List<Disposable> resources;
    private final BatchBackupManager backupManager;
    private final ScheduleFlushManager scheduleFlushManager;

    private RecordStore recordStore;
    private MessageStore indexStore;
    private MessageStore deadMessageStore;
    private MessageStore deadMessageContentStore;

    private MessageService messageService;



    public ServerWrapper(DynamicConfig config, DynamicConfig deadConfig) {
        this.config = new DefaultBackupConfig(config);
        this.deadConfig = new DefaultBackupConfig(deadConfig);
        this.resources = new ArrayList<>();
        this.backupManager = new BatchBackupManager();
        this.scheduleFlushManager = new ScheduleFlushManager();
        DicStore dicStore = new DbDicDao(false);
        this.dicService = new DbDicService(dicStore, SIX_DIGIT_FORMAT_PATTERN);
    }

    public void start() {
        try {
            final DynamicConfig localConfig = config.getDynamicConfig();
            final DynamicConfig deadLocalConfig = deadConfig.getDynamicConfig();
            int listenPort = localConfig.getInt(PORT_CONFIG, DEFAULT_PORT);
            final MetaServerLocator metaServerLocator = new MetaServerLocator(localConfig.getString(META_SERVER_ENDPOINT));
            BrokerRegisterService brokerRegisterService = new BrokerRegisterService(listenPort, metaServerLocator);
            brokerRegisterService.start();
			if (BrokerConfig.getBrokerRole() != BrokerRole.BACKUP) {
                LOG.error("Config error,({})'s role is not backup.", NetworkUtils.getLocalHostname());
                throw new IllegalArgumentException("Config error, the role is not backup");
            }
            config.setBrokerGroup(BrokerConfig.getBrokerName());
            register(localConfig, deadLocalConfig);
        } catch (Exception e) {
            LOG.error("backup server start up failed.", e);
            Throwables.propagate(e);
        }
    }

    private void register(final DynamicConfig config, final DynamicConfig deadConfig) {
        BrokerRole role = BrokerConfig.getBrokerRole();
        if(role != BrokerRole.BACKUP) throw new RuntimeException("Only support backup");

        final SlaveSyncClient slaveSyncClient = new SlaveSyncClient(config);
        final MasterSlaveSyncManager masterSlaveSyncManager = new MasterSlaveSyncManager(slaveSyncClient);

        StorageConfig storageConfig = dummyConfig(config);
        StorageConfig deadStorageConfig = dummyConfig(deadConfig);


        final CheckpointManager checkpointManager = new CheckpointManager(BrokerConfig.getBrokerRole(), storageConfig, null);

        final CheckpointManager deadCheckpointManager = new CheckpointManager(BrokerConfig.getBrokerRole(), deadStorageConfig, null);

        final BackupKeyGenerator keyGenerator = new BackupKeyGenerator(dicService);
        final KvStore.StoreFactory factory = new FactoryStoreImpl().createStoreFactory(config, dicService, keyGenerator);
        this.indexStore = factory.createMessageIndexStore();
        this.recordStore = factory.createRecordStore();
        this.deadMessageStore = factory.createDeadMessageStore();
        this.deadMessageContentStore = factory.createDeadMessageContentStore();

        IndexLog indexLog = new IndexLog(storageConfig, checkpointManager, Lists.newArrayList(checkpointManager, deadCheckpointManager));

        messageService = new MessageServiceImpl(config, indexStore, deadMessageStore, recordStore);

        FixedExecOrderEventBus.Listener<MessageQueryIndex> indexProcessor = getConstructIndexListener(keyGenerator
                , messageQueryIndex -> indexLog.commit(checkpointManager, messageQueryIndex.getCurrentOffset()));

        FixedExecOrderEventBus.Listener<MessageQueryIndex> deadMessageIndexProcessor = getConstructDeadIndexListener(keyGenerator
                , messageQueryIndex -> indexLog.commit(deadCheckpointManager, messageQueryIndex.getCurrentOffset()));

        final FixedExecOrderEventBus bus = new FixedExecOrderEventBus();
        bus.subscribe(MessageQueryIndex.class, indexProcessor);

        LogIterateService<MessageQueryIndex> iterateService = new LogIterateService<>("index", new StorageConfigImpl(config), indexLog, checkpointManager.getIndexIterateCheckpoint(), bus);
        IndexFileStore indexFileStore = new IndexFileStore(indexLog, config);
        scheduleFlushManager.register(indexFileStore);

        final FixedExecOrderEventBus deadBus = new FixedExecOrderEventBus();
        deadBus.subscribe(MessageQueryIndex.class, deadMessageIndexProcessor);

        LogIterateService<MessageQueryIndex> deadIterateService = new LogIterateService<>("dead-index", new StorageConfigImpl(config), indexLog, deadCheckpointManager.getIndexIterateCheckpoint(), deadBus);

        final IndexLogSyncDispatcher dispatcher = new IndexLogSyncDispatcher(indexLog);
        masterSlaveSyncManager.registerProcessor(dispatcher.getSyncType(), new BackupMessageLogSyncProcessor(dispatcher));

        // action
        final RocksDBStore rocksDBStore = new RocksDBStoreImpl(config);
        final BatchBackup<ActionRecord> recordBackup = new RecordBatchBackup(this.config, keyGenerator, rocksDBStore, recordStore);
        backupManager.registerBatchBackup(recordBackup);

        final SyncLogIterator<Action, ByteBuf> actionIterator = new ActionSyncLogIterator();
        BackupActionLogSyncProcessor actionLogSyncProcessor = new BackupActionLogSyncProcessor(checkpointManager, config, actionIterator, recordBackup);
        masterSlaveSyncManager.registerProcessor(SyncType.action, actionLogSyncProcessor);

        scheduleFlushManager.register(actionLogSyncProcessor);

        masterSlaveSyncManager.registerProcessor(SyncType.heartbeat, new HeartBeatProcessor(checkpointManager));


        scheduleFlushManager.scheduleFlush();
        backupManager.start();
        iterateService.start();
        deadIterateService.start();
        masterSlaveSyncManager.startSync();
        addResourcesInOrder(scheduleFlushManager, backupManager, masterSlaveSyncManager);
    }

    public MessageService getMessageService() {
        return messageService;
    }

    private StorageConfig dummyConfig(DynamicConfig config) {
        class BackupStorageConfig extends StorageConfigImpl {

            private BackupStorageConfig(DynamicConfig config) {
                super(config);
            }
        }
        return new BackupStorageConfig(config);
    }

    private FixedExecOrderEventBus.Listener<MessageQueryIndex> getConstructIndexListener(final BackupKeyGenerator keyGenerator, Consumer<MessageQueryIndex> consumer) {

        final BatchBackup<MessageQueryIndex> indexBackup = new MessageIndexBatchBackup(config, indexStore, keyGenerator);
        backupManager.registerBatchBackup(indexBackup);


        return new IndexEventBusListener(indexBackup, consumer);
    }

    private FixedExecOrderEventBus.Listener<MessageQueryIndex> getConstructDeadIndexListener(final BackupKeyGenerator keyGenerator, Consumer<MessageQueryIndex> consumer) {
        final BatchBackup<MessageQueryIndex> deadRecordBackup = new DeadRecordBatchBackup(recordStore, keyGenerator, config);
        backupManager.registerBatchBackup(deadRecordBackup);

        final BatchBackup<MessageQueryIndex> deadMessageBackup = new DeadMessageBatchBackup(deadMessageStore, keyGenerator, config);
        backupManager.registerBatchBackup(deadMessageBackup);

        final BatchBackup<MessageQueryIndex> deadMessageContentBackup = new DeadMessageContentBatchBackup(deadMessageContentStore, keyGenerator, config, messageService);
        backupManager.registerBatchBackup(deadMessageContentBackup);

        return new DeadMsgEventBusListener(deadMessageBackup, deadMessageContentBackup, deadRecordBackup, consumer);
    }

    private void addResourcesInOrder(Disposable resource, Disposable... resources) {
        this.resources.addAll(Lists.asList(resource, resources));
    }

    @Override
    public void destroy() {
        if (resources.isEmpty()) return;
        for (int i = resources.size() - 1; i >= 0; --i) {
            try {
                Disposable disposable = resources.get(i);
                if (disposable != null) disposable.destroy();
            } catch (Throwable e) {
                LOG.error("destroy resource failed", e);
            }
        }
    }
}
