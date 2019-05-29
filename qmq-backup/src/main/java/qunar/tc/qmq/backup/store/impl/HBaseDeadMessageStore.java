package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.HBaseClient;
import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.ResultIterable;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.util.BackupMessageKeyRangeBuilder;
import qunar.tc.qmq.backup.util.BackupMessageKeyRegexpBuilder;

import java.util.Date;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class HBaseDeadMessageStore extends HBaseMessageStore implements MessageStore {
    private byte[] TABLE;
    private DicService dicService;

    public HBaseDeadMessageStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client) {
        super(table, family, qualifiers, client);
    }

    @Override
    public ResultIterable<BackupMessage> findMessages(BackupQuery query) {
        if (isInvalidate(query)) return EMPTY_RESULT;
        makeUp(query);
        return null;
    }

    private ResultIterable<BackupMessage> findDeadMessagesInternal(BackupQuery query) {
        final String subject = query.getSubject();
        final Date msgCreateTimeBegin = query.getMsgCreateTimeBegin();
        final Date msgCreateTimeEnd = query.getMsgCreateTimeEnd();
        final int len = 200;
        final String start = (String) query.getStart();
        final String consumerGroup = query.getConsumerGroup();

        final String subjectId = dicService.name2Id(subject);
        final String consumerGroupId = dicService.name2Id(consumerGroup);

        final String keyRegexp = BackupMessageKeyRegexpBuilder.buildDeadRegexp(subjectId, consumerGroupId);
        final String startKey = BackupMessageKeyRangeBuilder.buildDeadStartKey(start, subjectId, consumerGroupId, msgCreateTimeEnd);
        final String endKey = BackupMessageKeyRangeBuilder.buildDeadEndKey(subjectId, consumerGroupId, msgCreateTimeBegin);

        final ResultIterable<BackupMessage> resultIterable = new ResultIterable<>();
        getMessageFromHBase(subject, TABLE, resultIterable, keyRegexp, startKey, endKey, len);
        return resultIterable;
    }
}
