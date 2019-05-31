package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.HBaseClient;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.util.BackupMessageKeyRangeBuilder;
import qunar.tc.qmq.backup.util.BackupMessageKeyRegexpBuilder;

import java.util.Date;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class HBaseIndexStore extends AbstractHBaseMessageStore {
    private DicService dicService;

    HBaseIndexStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client, DicService dicService) {
        super(table, family, qualifiers, client);
        this.dicService = dicService;
    }

    @Override
    protected MessageQueryResult findMessagesInternal(final BackupQuery query) {
        final String subject = query.getSubject();
        final Date msgCreateTimeBegin = query.getMsgCreateTimeBegin();
        final Date msgCreateTimeEnd = query.getMsgCreateTimeEnd();
        final int len = query.getLen();
        final String start = (String) query.getStart();
        final String messageId = query.getMessageId();
        String subjectId;
        byte[] table;
        if (!query.isDelay()) {
            subjectId = dicService.name2Id(subject);
            table = this.table;
        } else {
            return EMPTY_RESULT;
        }

        final MessageQueryResult messageQueryResult = new MessageQueryResult();

        final String keyRegexp;
        final String startKey;
        final String endKey;

        keyRegexp = BackupMessageKeyRegexpBuilder.build(subjectId, messageId);
        startKey = BackupMessageKeyRangeBuilder.buildStartKey(start, subjectId, msgCreateTimeEnd);
        endKey = BackupMessageKeyRangeBuilder.buildEndKey(subjectId, msgCreateTimeBegin);
        getMessageFromHBase(subject, table, messageQueryResult, keyRegexp, startKey, endKey, len);

        return messageQueryResult;
    }

}
