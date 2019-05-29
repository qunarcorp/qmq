package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.ResultIterable;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public interface MessageStore extends KvStore {
    ResultIterable<BackupMessage> findMessages(BackupQuery query);
}
