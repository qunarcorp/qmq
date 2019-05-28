package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.utils.CharsetUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.*;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-07 19:13
 */
public class HBaseStoreFactory implements KvStore.StoreFactory {
    private static final short CLIENT_FLUSH_INTERVAL = (short) TimeUnit.SECONDS.toMillis(1);
    private static final int CLIENT_BUFFER_SIZE = 8 * 1024;

    private final HBaseClient client;

    HBaseStoreFactory(DynamicConfig config) {
        final Config HBaseConfig = from(config);
        this.client = new HBaseClient(HBaseConfig);
        this.client.setFlushInterval(CLIENT_FLUSH_INTERVAL);
        this.client.setIncrementBufferSize(CLIENT_BUFFER_SIZE);
    }

    private static Config from(DynamicConfig config) {
        Map<String, String> map = config.asMap();
        final Config hbaseConfig = new Config();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            hbaseConfig.overrideConfig(entry.getKey(), entry.getValue());
        }
        return hbaseConfig;
    }

    @Override
    public KvStore createMessageIndexStore(String workTable) {
        byte[] table = CharsetUtils.toUTF8Bytes(workTable);
        return new DefaultHBaseStore(table, B_FAMILY, B_MESSAGE_QUALIFIERS, client);
    }

    @Override
    public KvStore createRecordStore(String workTable) {
        byte[] table = CharsetUtils.toUTF8Bytes(workTable);
        return new DefaultHBaseStore(table, R_FAMILY, B_RECORD_QUALIFIERS, client);
    }

    @Override
    public KvStore createDeadMessageStore(String workTable) {
        byte[] table = CharsetUtils.toUTF8Bytes(workTable);
        return new DefaultHBaseStore(table, B_FAMILY, B_MESSAGE_QUALIFIERS, client);
    }
}
