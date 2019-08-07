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

import java.util.Date;

import org.hbase.async.HBaseClient;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.util.BackupMessageKeyRangeBuilder;
import qunar.tc.qmq.backup.util.BackupMessageKeyRegexpBuilder;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class HBaseDeadMessageStore extends AbstractHBaseMessageStore implements MessageStore {
    private DicService dicService;

    HBaseDeadMessageStore(byte[] table, byte[] family, byte[][] qualifiers, HBaseClient client, DicService dicService) {
        super(table, family, qualifiers, client);
        this.dicService = dicService;
    }

    @Override
    protected MessageQueryResult findMessagesInternal(BackupQuery query) {
        final String subject = query.getSubject();
        final Date msgCreateTimeBegin = query.getMsgCreateTimeBegin();
        final Date msgCreateTimeEnd = query.getMsgCreateTimeEnd();
        final int len = query.getLen();
        final String start = query.getStart();
        final String consumerGroup = query.getConsumerGroup();

        final String subjectId = dicService.name2Id(subject);
        final String consumerGroupId = dicService.name2Id(consumerGroup);

        final String keyRegexp = BackupMessageKeyRegexpBuilder.buildDeadRegexp(subjectId, consumerGroupId);
        final String startKey = BackupMessageKeyRangeBuilder.buildDeadStartKey(start, subjectId, consumerGroupId, msgCreateTimeEnd);
        final String endKey = BackupMessageKeyRangeBuilder.buildDeadEndKey(subjectId, consumerGroupId, msgCreateTimeBegin);

        final MessageQueryResult messageQueryResult = new MessageQueryResult();
        getMessageFromHBase(subject, table, messageQueryResult, keyRegexp, startKey, endKey, len);
        return messageQueryResult;
    }
}
