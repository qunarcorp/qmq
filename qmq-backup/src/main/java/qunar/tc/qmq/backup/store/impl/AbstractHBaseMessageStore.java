/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.backup.store.impl;

import com.google.common.base.Strings;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupMessageMeta;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.util.KeyValueList;
import qunar.tc.qmq.backup.util.KeyValueListImpl;
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
public abstract class AbstractHBaseMessageStore<T> extends HBaseStore implements MessageStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHBaseMessageStore.class);

    final MessageQueryResult<T> EMPTY_RESULT = new MessageQueryResult<T>();

    AbstractHBaseMessageStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client) {
        super(table, family, qualifiers, client);
    }

    @Override
    public MessageQueryResult<T> findMessages(BackupQuery query) {
        if (isInvalidate(query)) return EMPTY_RESULT;
        makeUp(query);
        return findMessagesInternal(query);
    }

    protected abstract MessageQueryResult<T> findMessagesInternal(BackupQuery query);

    void scan(final String subject,
              final byte[] table,
              final MessageQueryResult<T> messageQueryResult,
              final String keyRegexp,
              final String startKey,
              final String endKey,
              final int maxResults) {
        List<T> result;
        try {
            result = scan(table, keyRegexp, startKey, endKey, maxResults + 1, 0, B_FAMILY, B_MESSAGE_QUALIFIERS, kvs -> {
                KeyValueList<KeyValue> kvl = new KeyValueListImpl(kvs);
                messageQueryResult.setNext(new String(kvl.getKey(), CharsetUtil.UTF_8));
                String version = new String(kvl.getValue(VERSION));
                byte[] value = kvl.getValue(CONTENT);
                T item = (T)getItem(subject, version, value);
                if (item == null) {
                    Metrics.counter("message.content.missing").inc();
                    LOGGER.info("Message content missing");
                }
                return item;
            });
        } catch (Exception e) {
            LOGGER.error("Failed to get messages from hbase.", e);
            messageQueryResult.setList(Collections.emptyList());
            return;
        }
        int size = result.size();
        LOGGER.info("Found {} metas from HBase.", size);
        slim(result, messageQueryResult, maxResults);
        messageQueryResult.setList(result);
    }

    private  <T> void slim(final List<T> messageRowKeys, final MessageQueryResult messageQueryResult, final int maxResults) {
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

    protected T getItem(String subject, String version, byte[] value) {
        BackupMessageMeta meta = getMessageMeta(subject, version, value);
        final String brokerGroup = meta.getBrokerGroup();
        final long sequence = meta.getSequence();
        final String messageId = meta.getMessageId();
        return (T)new MessageQueryResult.MessageMeta(subject, meta.getPartitionName(), messageId, sequence, meta.getCreateTime(), brokerGroup);
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
}
