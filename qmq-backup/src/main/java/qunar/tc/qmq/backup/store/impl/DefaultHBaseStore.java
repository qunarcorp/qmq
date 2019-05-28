package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultHBaseStore extends AbstractHBaseStore {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultHBaseStore.class);

    private volatile boolean isClosed = false;
    private final HBaseClient client;

    public DefaultHBaseStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client) {
        super(table, family, qualifiers);
        this.client = client;
    }

    @Override
    protected void doBatchSave(byte[] table, byte[][] keys, byte[] family, byte[][] qualifiers, byte[][][] values) {
        for (int i = 0; i < keys.length; ++i) {
            doSave(table, keys[i], family, qualifiers, values[i]);
        }
    }

    private void doSave(byte[] table, byte[] key, byte[] family, byte[][] qualifiers, byte[][] value) {
        PutRequest request = new PutRequest(table, key, family, qualifiers, value);
        client.put(request).addBoth(input -> {
            if (input instanceof Throwable) {
                LOG.error("put backup message failed.", input);
            }
            return null;
        });
    }

    @Override
    public void close() {
        if (isClosed) return;
        client.shutdown();
        isClosed = true;
    }
}
