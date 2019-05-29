package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.base.RecordQuery;
import qunar.tc.qmq.backup.base.RecordResult;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public interface RecordStore extends KvStore {
    RecordResult findRecords(RecordQuery query);
}
