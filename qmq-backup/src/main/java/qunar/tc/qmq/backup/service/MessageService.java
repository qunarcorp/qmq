package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.RecordResult;
import qunar.tc.qmq.backup.base.ResultIterable;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-20 15:24
 */
public interface MessageService {

    ResultIterable<BackupMessage> findMessages(BackupQuery query);

    ResultIterable<BackupMessage> findDeadMessages(BackupQuery query);

    BackupMessage findMessage(BackupQuery query);

    RecordResult findRecords(BackupQuery query);
}
