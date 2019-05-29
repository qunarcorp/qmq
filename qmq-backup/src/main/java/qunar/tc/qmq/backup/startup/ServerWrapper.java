package qunar.tc.qmq.backup.startup;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.ActionRecord;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.config.DefaultBackupConfig;
import qunar.tc.qmq.backup.service.*;
import qunar.tc.qmq.backup.service.impl.*;
import qunar.tc.qmq.backup.store.DicStore;
import qunar.tc.qmq.backup.store.KvStore;
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
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.sync.MasterSlaveSyncManager;
import qunar.tc.qmq.sync.SlaveSyncClient;
import qunar.tc.qmq.sync.SyncType;
import qunar.tc.qmq.utils.NetworkUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.INDEX_LOG_DIR_PATH_CONFIG_KEY;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.OFFSET_FILE_PATH_CONFIG_KEY;
import static qunar.tc.qmq.backup.service.DicService.SIX_DIGIT_FORMAT_PATTERN;
import static qunar.tc.qmq.constants.BrokerConstants.*;

public class ServerWrapper implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerWrapper.class);

    private final BackupConfig config;
    private final DicService dicService;
    private final List<Disposable> resources;
    private final BatchBackupManager backupManager;
    private final ScheduleFlushManager scheduleFlushManager;

    private KvStore recordStore;
    private KvStore indexStore;
    private KvStore deadMessageStore;

    public ServerWrapper(DynamicConfig config) {
        this.config = new DefaultBackupConfig(config);
        this.resources = new ArrayList<>();
        this.backupManager = new BatchBackupManager();
        this.scheduleFlushManager = new ScheduleFlushManager();
        DicStore dicStore = new DbDicDao(config, false);
        this.dicService = new DbDicService(dicStore, SIX_DIGIT_FORMAT_PATTERN);

    }

    public void start() {
        try {
            final DynamicConfig localConfig = config.getDynamicConfig();
            int listenPort = localConfig.getInt(PORT_CONFIG, DEFAULT_PORT);
            final MetaServerLocator metaServerLocator = new MetaServerLocator(localConfig.getString(META_SERVER_ENDPOINT));
            BrokerRegisterService brokerRegisterService = new BrokerRegisterService(listenPort, metaServerLocator);
            brokerRegisterService.start();
            if (BrokerConfig.getBrokerRole() != BrokerRole.BACKUP) {
                LOG.error("配置中心参数配置错误，当前机器({})对应的角色不是backup。请检查配置。", NetworkUtils.getLocalHostname());
                throw new IllegalArgumentException("配置中心参数配置错误，当前机器对应的角色不是backup");
            }
            config.setBrokerGroup(BrokerConfig.getBrokerName());
            register(localConfig);
        } catch (Exception e) {
            LOG.error("backup server start up failed.", e);
            Throwables.propagate(e);
        }
    }

    private void register(final DynamicConfig config) {
        final KvStore.StoreFactory factory = new FactoryStoreImpl().createStoreFactory(config);
        final SlaveSyncClient slaveSyncClient = new SlaveSyncClient(config);
        final MasterSlaveSyncManager masterSlaveSyncManager = new MasterSlaveSyncManager(slaveSyncClient);

        final File indexLogDir = new File(config.getString(INDEX_LOG_DIR_PATH_CONFIG_KEY));
        ensureDir(indexLogDir);
        final File checkpointDir = new File(config.getString(OFFSET_FILE_PATH_CONFIG_KEY));
        ensureDir(checkpointDir);

        StorageConfig storageConfig = dummyConfig(indexLogDir.getAbsolutePath(), checkpointDir.getAbsolutePath());
        final CheckpointManager checkpointManager = new CheckpointManager(BrokerConfig.getBrokerRole(), storageConfig, null);
        final FixedExecOrderEventBus bus = new FixedExecOrderEventBus();
        BrokerRole role = BrokerConfig.getBrokerRole();
        IndexLog log;
        if (role == BrokerRole.BACKUP) {
            final BackupKeyGenerator keyGenerator = new BackupKeyGenerator(dicService);
            this.indexStore = factory.createMessageIndexStore();
            this.recordStore = factory.createRecordStore();
            this.deadMessageStore = factory.createDeadMessageStore();
            log = new IndexLog(storageConfig, checkpointManager);
            final IndexLogSyncDispatcher dispatcher = new IndexLogSyncDispatcher(log);

            bus.subscribe(MessageQueryIndex.class, getConstructIndexListener(keyGenerator
                    , messageQueryIndex -> log.setDeleteTo(messageQueryIndex.getCurrentOffset())));
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

        } else if (role == BrokerRole.DELAY_BACKUP) {
            throw new RuntimeException("check the role is correct, only backup is allowed.");
        } else {
            throw new RuntimeException("check the role is correct, only backup is allowed.");
        }

        IndexLogIterateService iterateService = new IndexLogIterateService(config, log, checkpointManager, bus);
        IndexFileStore indexFileStore = new IndexFileStore(log, config);
        scheduleFlushManager.register(indexFileStore);

        scheduleFlushManager.scheduleFlush();
        backupManager.start();
        iterateService.start();
        masterSlaveSyncManager.startSync();
        addResourcesInOrder(scheduleFlushManager, backupManager, iterateService, masterSlaveSyncManager);
    }

    private StorageConfig dummyConfig(String indexDir, String checkpointDir) {
        return new StorageConfig() {
            @Override
            public String getCheckpointStorePath() {
                return checkpointDir;
            }

            @Override
            public String getMessageLogStorePath() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getMessageLogRetentionMs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getConsumerLogStorePath() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getConsumerLogRetentionMs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getLogRetentionCheckIntervalSeconds() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getPullLogStorePath() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getPullLogRetentionMs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getActionLogStorePath() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getIndexLogStorePath() {
                return indexDir;
            }

            @Override
            public boolean isDeleteExpiredLogsEnable() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLogRetentionMs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getRetryDelaySeconds() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getCheckpointRetainCount() {
                return 3;
            }

            @Override
            public long getActionCheckpointInterval() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getMessageCheckpointInterval() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private void ensureDir(final File file) {
        if (!file.exists()) {
            if (file.mkdirs()) {
                LOG.info("create dir {}", file.getAbsolutePath());
            } else {
                LOG.error("create dir {} error.", file.getAbsolutePath());
            }
        }
    }

    private FixedExecOrderEventBus.Listener<MessageQueryIndex> getConstructIndexListener(final BackupKeyGenerator keyGenerator, Consumer<MessageQueryIndex> consumer) {
        final BatchBackup<MessageQueryIndex> deadRecordBackup = new DeadRecordBatchBackup(recordStore, keyGenerator, config);
        backupManager.registerBatchBackup(deadRecordBackup);
        final BatchBackup<MessageQueryIndex> deadMessageBackup = new DeadMessageBatchBackup(deadMessageStore, keyGenerator, config);
        backupManager.registerBatchBackup(deadMessageBackup);
        final BatchBackup<MessageQueryIndex> indexBackup = new MessageIndexBatchBackup(config, indexStore, keyGenerator);
        backupManager.registerBatchBackup(indexBackup);
        return new IndexEventBusListener(deadMessageBackup, deadRecordBackup, indexBackup, consumer);
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
