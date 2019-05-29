package qunar.tc.qmq.backup.store.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.hbase.async.HBaseClient;
import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupMessageMeta;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.ResultIterable;
import qunar.tc.qmq.base.BaseMessage;

import java.time.LocalDateTime;
import java.util.List;

import static qunar.tc.qmq.backup.util.DateTimeUtils.date2LocalDateTime;
import static qunar.tc.qmq.backup.util.DateTimeUtils.localDateTime2Date;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class HBaseMessageStore extends HBaseStore {

    protected static final ResultIterable<BackupMessage> EMPTY_RESULT = new ResultIterable<>();

    public HBaseMessageStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client) {
        super(table, family, qualifiers, client);
    }

    protected void getMessageFromHBase(final String subject, final byte[] table, final ResultIterable<BackupMessage> resultIterable, final String keyRegexp, final String startKey, final String endKey
            , final int maxResults) {

    }

    private List<BackupMessage> getMessagesWithMeta(final String subject, final List<BackupMessageMeta> metaList) {
        final List<BackupMessage> messages = Lists.newArrayListWithCapacity(metaList.size());
        for (BackupMessageMeta meta : metaList) {
            final String brokerGroup = meta.getBrokerGroup();
            final long sequence = meta.getSequence();
            final String messageId = meta.getMessageId();
            final BackupMessage message = new BackupMessage(messageId, subject);
            message.setSequence(sequence);
            message.setBrokerGroup(brokerGroup);
            message.setProperty(BaseMessage.keys.qmq_createTime, meta.getCreateTime());
            messages.add(message);
        }

        return messages;
    }

    protected void makeUp(final BackupQuery query) {
        if (query.getMsgCreateTimeEnd() == null) {
            LocalDateTime now = LocalDateTime.now();
            query.setMsgCreateTimeEnd(localDateTime2Date(now));
        }
        if (query.getMsgCreateTimeBegin() == null) {
            LocalDateTime begin = date2LocalDateTime(query.getMsgCreateTimeEnd()).plusDays(-30);
            query.setMsgCreateTimeBegin(localDateTime2Date(begin));
        }
        if (!Strings.isNullOrEmpty(query.getMessageId())) {
            query.setLen(1);
        }
    }

    protected boolean isInvalidate(final BackupQuery query) {
        return query == null || Strings.isNullOrEmpty(query.getSubject());
    }
}
