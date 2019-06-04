package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public interface MessageStore extends KvStore {
    MessageQueryResult findMessages(BackupQuery query);
}
