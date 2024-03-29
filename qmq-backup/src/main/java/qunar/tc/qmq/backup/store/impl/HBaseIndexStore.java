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

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.jboss.netty.util.CharsetUtil;
import qunar.tc.qmq.backup.base.BackupMessageMeta;
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
public class HBaseIndexStore extends AbstractHBaseMessageStore<MessageQueryResult.MessageMeta> {
    private DicService dicService;

    HBaseIndexStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client, DicService dicService) {
        super(table, family, qualifiers, client, dicService);
        this.dicService = dicService;
    }

    @Override
    protected MessageQueryResult findMessagesInternal(final BackupQuery query) {
        final String subject = query.getSubject();
        final Date msgCreateTimeBegin = query.getMsgCreateTimeBegin();
        final Date msgCreateTimeEnd = query.getMsgCreateTimeEnd();
        final int len = query.getLen();
        final String start = query.getStart();
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

    @Override
    protected BackupMessageMeta getMessageMeta(byte[] key, byte[] value) {
        if (value == null || value.length <= 0) {
            return null;
        }
        try {
            long sequence = Bytes.getLong(value, 0);
            long createTime = Bytes.getLong(value, 8);
            int brokerGroupLength = Bytes.getInt(value, 16);
            if (brokerGroupLength > 200) {
                return null;
            }
            byte[] brokerGroupBytes = new byte[brokerGroupLength];
            System.arraycopy(value, 20, brokerGroupBytes, 0, brokerGroupLength);
            int messageIdLength = value.length - 20 - brokerGroupLength;
            byte[] messageIdBytes = new byte[messageIdLength];
            System.arraycopy(value, 20 + brokerGroupLength, messageIdBytes, 0, messageIdLength);
            BackupMessageMeta meta = new BackupMessageMeta(sequence, new String(brokerGroupBytes, CharsetUtil.UTF_8), new String(messageIdBytes, CharsetUtil.UTF_8));
            meta.setCreateTime(createTime);
            return meta;
        } catch (Exception ignored) {
        }
        return null;
    }

}
