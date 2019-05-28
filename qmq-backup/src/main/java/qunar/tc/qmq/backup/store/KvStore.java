package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.configuration.DynamicConfig;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-05 18:55
 */
public interface KvStore extends AutoCloseable {
    void batchSave(byte[][] key, byte[][][] value);

    interface StoreFactory {
        KvStore createMessageIndexStore(String workTable);

        KvStore createRecordStore(String workTable);

        KvStore createDeadMessageStore(String workTable);
    }

    interface FactoryStore {
        StoreFactory createStoreFactory(DynamicConfig config);
    }
}
