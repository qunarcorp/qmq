package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.backup.base.*;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-20 15:24
 */
public interface MessageService {

    MessageQueryResult findMessages(BackupQuery query);

    MessageQueryResult findDeadMessages(BackupQuery query);

    BackupMessage findMessage(BackupQuery query);

    RecordQueryResult findRecords(RecordQuery query);
}
