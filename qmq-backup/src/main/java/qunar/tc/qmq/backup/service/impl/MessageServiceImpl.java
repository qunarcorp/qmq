package qunar.tc.qmq.backup.service.impl;

import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.RecordResult;
import qunar.tc.qmq.backup.base.ResultIterable;
import qunar.tc.qmq.backup.service.MessageService;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class MessageServiceImpl implements MessageService {


    @Override
    public ResultIterable<BackupMessage> findMessages(BackupQuery query) {
        return null;
    }

    @Override
    public ResultIterable<BackupMessage> findDeadMessages(BackupQuery query) {
        return null;
    }

    @Override
    public BackupMessage findMessage(BackupQuery query) {
        return null;
    }

    @Override
    public RecordResult findRecords(BackupQuery query) {
        return null;
    }
}
