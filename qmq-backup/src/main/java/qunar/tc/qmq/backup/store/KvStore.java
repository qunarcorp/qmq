package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.configuration.DynamicConfig;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-05 18:55
 */
public interface KvStore extends AutoCloseable {
    void batchSave(byte[][] key, byte[][][] value);

    interface StoreFactory {
        MessageStore createMessageIndexStore();

        RecordStore createRecordStore();

        MessageStore createDeadMessageStore();
    }

    interface FactoryStore {
        StoreFactory createStoreFactory(DynamicConfig config);
    }
}
