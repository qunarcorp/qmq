package qunar.tc.qmq.backup.store.impl;

import qunar.tc.qmq.backup.base.UnsupportedArgumentsException;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.configuration.DynamicConfig;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_STORE_FACTORY_TYPE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.STORE_FACTORY_TYPE_CONFIG_KEY;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-11 20:32
 */
public class FactoryStoreImpl implements KvStore.FactoryStore {

    @Override
    public KvStore.StoreFactory createStoreFactory(DynamicConfig config, DicService dicService, BackupKeyGenerator keyGenerator) {
        String storeType = config.getString(STORE_FACTORY_TYPE_CONFIG_KEY, DEFAULT_STORE_FACTORY_TYPE);
        return StoreFactoryType.fromCode(storeType).createStoreFactory(config, dicService, keyGenerator);
    }

    private enum StoreFactoryType {
        HBASE("hbase", HBaseStoreFactory::new),
        MYSQL("mysql", (config, dicService, keyGenerator) -> {
            throw new UnsupportedOperationException();
        }),
        OTHER("other", (config, dicService, keyGenerator) -> {
            throw new UnsupportedArgumentsException("Wrong store.type in config.properties");
        });

        private final String type;
        private final KvStore.FactoryStore factoryStore;

        StoreFactoryType(String type, KvStore.FactoryStore factoryStore) {
            this.type = type;
            this.factoryStore = factoryStore;
        }

        static KvStore.FactoryStore fromCode(String type) {
            for (StoreFactoryType storeFactoryType : StoreFactoryType.values()) {
                if (storeFactoryType.type.equals(type)) return storeFactoryType.factoryStore;
            }

            return OTHER.factoryStore;
        }
    }
}
