package qunar.tc.qmq.backup.store.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupMessageMeta;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.util.KeyValueList;
import qunar.tc.qmq.backup.util.KeyValueListImpl;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.metrics.Metrics;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static qunar.tc.qmq.backup.util.DateTimeUtils.date2LocalDateTime;
import static qunar.tc.qmq.backup.util.DateTimeUtils.localDateTime2Date;
import static qunar.tc.qmq.backup.util.HBaseValueDecoder.getMessageMeta;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public abstract class AbstractHBaseMessageStore extends HBaseStore implements MessageStore {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseMessageStore.class);

    static final MessageQueryResult EMPTY_RESULT = new MessageQueryResult();

    AbstractHBaseMessageStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client) {
        super(table, family, qualifiers, client);
    }

    void getMessageFromHBase(final String subject, final byte[] table, final MessageQueryResult messageQueryResult, final String keyRegexp, final String startKey, final String endKey
            , final int maxResults) {
        List<BackupMessageMeta> metas;
        try {
            metas = scan(table, keyRegexp, startKey, endKey, maxResults + 1, 0, B_FAMILY, B_MESSAGE_QUALIFIERS, kvs -> {
                KeyValueList<KeyValue> kvl = new KeyValueListImpl(kvs);
                messageQueryResult.setNext(new String(kvl.getKey(), CharsetUtil.UTF_8));
                byte[] value = kvl.getValue(CONTENT);
                BackupMessageMeta meta = getMessageMeta(value);
                if (meta == null) {
                    Metrics.counter("message.content.missing").inc();
                    LOG.info("Message content missing");
                }
                return meta;
            });
        } catch (Exception e) {
            LOG.error("Failed to get messages from hbase.", e);
            messageQueryResult.setList(Collections.emptyList());
            return;
        }
        int size = metas.size();
        LOG.info("Found {} metas from HBase.", size);
        slim(metas, messageQueryResult, maxResults);

        List<BackupMessage> messages = getMessagesWithMeta(subject, metas);
        messageQueryResult.setList(messages);
    }

    private <T> void slim(final List<T> messageRowKeys, final MessageQueryResult messageQueryResult, final int maxResults) {
        int size = messageRowKeys.size();
        if (maxResults > 0) {
            if (size <= maxResults) {
                messageQueryResult.setNext(null);
            }
            if (size > maxResults) {
                messageRowKeys.remove(size - 1);
            }
        }
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

    private void makeUp(final BackupQuery query) {
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

    private boolean isInvalidate(final BackupQuery query) {
        return query == null || Strings.isNullOrEmpty(query.getSubject());
    }

    @Override
    public MessageQueryResult findMessages(BackupQuery query) {
        if (isInvalidate(query)) return EMPTY_RESULT;
        makeUp(query);
        return findMessagesInternal(query);
    }

    protected abstract MessageQueryResult findMessagesInternal(BackupQuery query);
}
