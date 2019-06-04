package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.backup.base.*;

import java.util.concurrent.CompletableFuture;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-20 15:24
 */
public interface MessageService {

    CompletableFuture<MessageQueryResult> findMessages(BackupQuery query);

    CompletableFuture<MessageQueryResult> findDeadMessages(BackupQuery query);

    CompletableFuture<BackupMessage> findMessage(BackupQuery query);

    CompletableFuture<RecordQueryResult> findRecords(RecordQuery query);
}
