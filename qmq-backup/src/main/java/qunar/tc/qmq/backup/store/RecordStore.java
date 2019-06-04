package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.base.RecordQuery;
import qunar.tc.qmq.backup.base.RecordQueryResult;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public interface RecordStore extends KvStore {
    RecordQueryResult findRecords(RecordQuery query);
}
