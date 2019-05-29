package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.HBaseClient;
import qunar.tc.qmq.backup.base.RecordQuery;
import qunar.tc.qmq.backup.base.RecordResult;
import qunar.tc.qmq.backup.store.RecordStore;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class HBaseRecordStore extends HBaseStore implements RecordStore {
    public HBaseRecordStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client) {
        super(table, family, qualifiers, client);
    }

    @Override
    public RecordResult findRecords(RecordQuery query) {
        return null;
    }
}
