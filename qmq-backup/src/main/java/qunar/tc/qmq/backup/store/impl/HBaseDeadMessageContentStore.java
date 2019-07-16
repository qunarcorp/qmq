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

import static qunar.tc.qmq.backup.util.HBaseValueDecoder.getMessage;
import static qunar.tc.qmq.backup.util.HBaseValueDecoder.getMessageMeta;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupMessageMeta;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.util.BackupMessageKeyRangeBuilder;
import qunar.tc.qmq.backup.util.BackupMessageKeyRegexpBuilder;
import qunar.tc.qmq.backup.util.KeyValueList;
import qunar.tc.qmq.backup.util.KeyValueListImpl;
import qunar.tc.qmq.metrics.Metrics;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class HBaseDeadMessageContentStore extends AbstractHBaseMessageStore<BackupMessage> implements MessageStore {

    private final Logger LOG = LoggerFactory.getLogger(HBaseDeadMessageContentStore.class);

    private DicService dicService;

    HBaseDeadMessageContentStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client, DicService dicService) {
        super(table, family, qualifiers, client);
        this.dicService = dicService;
    }

    @Override
    protected MessageQueryResult findMessagesInternal(BackupQuery query) {
        final String subject = query.getSubject();
        final Date msgCreateTimeBegin = query.getMsgCreateTimeBegin();
        final Date msgCreateTimeEnd = query.getMsgCreateTimeEnd();
        final int len = query.getLen();
        final String start = (String) query.getStart();
        final String consumerGroup = query.getConsumerGroup();

        final String subjectId = dicService.name2Id(subject);
        final String consumerGroupId = dicService.name2Id(consumerGroup);
        final String messageId = query.getMessageId();

        final String keyRegexp = BackupMessageKeyRegexpBuilder.buildDeadContentRegexp(subjectId, consumerGroupId,  messageId);
        final String startKey = BackupMessageKeyRangeBuilder.buildDeadContentStartKey(start, subjectId, consumerGroupId, messageId, msgCreateTimeEnd);
        final String endKey = BackupMessageKeyRangeBuilder.buildDeadContentEndKey(subjectId, consumerGroupId, messageId, msgCreateTimeBegin);

        final MessageQueryResult<BackupMessage> messageQueryResult = new MessageQueryResult();
        getMessageFromHBase(subject, table, messageQueryResult, keyRegexp, startKey, endKey, len);
        return messageQueryResult;
    }

    void getMessageFromHBase(final String subject, final byte[] table, final MessageQueryResult messageQueryResult, final String keyRegexp, final String startKey, final String endKey
            , final int maxResults) {
        List<BackupMessage> backupMessages;
        try {
            backupMessages = scan(table, keyRegexp, startKey, endKey, maxResults + 1, 0, B_FAMILY, B_MESSAGE_QUALIFIERS, kvs -> {
                KeyValueList<KeyValue> kvl = new KeyValueListImpl(kvs);
                messageQueryResult.setNext(new String(kvl.getKey(), CharsetUtil.UTF_8));
                byte[] value = kvl.getValue(CONTENT);
                BackupMessage message = null;
                try {
                    message = getMessage(value);
                }
                catch (Exception e) {
                    LOG.warn("decode msg error!", e);
                }
                if (message == null) {
                    Metrics.counter("message.content.missing").inc();
                    LOG.info("Message content missing");
                }
                return message;
            });
        } catch (Exception e) {
            LOG.error("Failed to get messages from hbase.", e);
            messageQueryResult.setList(Collections.emptyList());
            return;
        }
        int size = backupMessages.size();
        LOG.info("Found {} backupMessagesContent from HBase.", size);
        slim(backupMessages, messageQueryResult, maxResults);

        messageQueryResult.setList(backupMessages);
    }
}
