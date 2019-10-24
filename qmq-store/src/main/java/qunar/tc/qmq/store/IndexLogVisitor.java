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

package qunar.tc.qmq.store;

import java.nio.ByteBuffer;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.BackupConstants;
import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.utils.PayloadHolderUtils;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class IndexLogVisitor extends AbstractLogVisitor<MessageQueryIndex> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexLogVisitor.class);

    IndexLogVisitor(final LogManager logManager, final long startOffset) {
        super(logManager, startOffset);
    }

    @Override
    protected LogVisitorRecord<MessageQueryIndex> readOneRecord(SegmentBuffer segmentBuffer) {
        final ByteBuffer buffer = segmentBuffer.getBuffer();
        final int startPos = buffer.position();

        // sequence
        if (buffer.remaining() < Long.BYTES) {
            return retNoMore();
        }

        // sequence
        long sequence = buffer.getLong();
        //小于零表示到了文件末尾，全部是填充数据
        if (sequence < 0) {
            return retNoMore();
        }

        // createTime
        if (buffer.remaining() < Long.BYTES) {
            return retNoMore();
        }
        long createTime = buffer.getLong();

        // subject
        if (buffer.remaining() < Short.BYTES) {
            return retNoMore();
        }
        short subjectLen = buffer.getShort();
        if (buffer.remaining() < subjectLen) {
            return retNoMore();
        }
        String subject = PayloadHolderUtils.readString(subjectLen, buffer);

        // msgId
        if (buffer.remaining() < Short.BYTES) {
            return retNoMore();
        }
        short messageIdLen = buffer.getShort();
        if (buffer.remaining() < messageIdLen) {
            return retNoMore();
        }
        String messageId = PayloadHolderUtils.readString(messageIdLen, buffer);

        int positionV1 = buffer.position();

        int currentPosition = positionV1;

        String partitionName = subject;

        ReadStringResult readSyncFlagResult = readString(buffer);

        if (readSyncFlagResult.isNoMore) {
            return retNoMore();
        }

        if (!BackupConstants.SYNC_V2_FLAG.equals(readSyncFlagResult.data)) {
            currentPosition = positionV1;
            buffer.position(positionV1);
            LOG.debug("消息中没包含flag: {}，属于老消息，回退buffer下标", BackupConstants.SYNC_V2_FLAG);
        } else {

            // partitionName
            ReadStringResult readPartitionNameResult = readString(buffer);

            if (readPartitionNameResult.isNoMore) {
                return retNoMore();
            }

            partitionName = readPartitionNameResult.data;

            currentPosition = buffer.position();
            LOG.debug("消息中包含flag: {}，解析到的partitionName为：{}", BackupConstants.SYNC_V2_FLAG, partitionName);
        }


        MessageQueryIndex index = new MessageQueryIndex(subject, messageId, partitionName, sequence, createTime);
        incrVisitedBufferSize(currentPosition - startPos);
        index.setCurrentOffset(getStartOffset() + visitedBufferSize());
        return LogVisitorRecord.data(index);
    }

    private ReadStringResult readString(ByteBuffer buffer) {
        short len = buffer.getShort();
        if (len < 0 || buffer.remaining() < len) {
            return new ReadStringResult(true, null);
        }

        return new ReadStringResult(false, PayloadHolderUtils.readString(len, buffer));
    }

    private static class ReadStringResult {
        private final boolean isNoMore;
        private final String data;

        private ReadStringResult(boolean isNoMore, String data) {
            this.isNoMore = isNoMore;
            this.data = data;
        }
    }


    private LogVisitorRecord<MessageQueryIndex> retNoMore() {
        setVisitedBufferSize(getBufferSize());
        return LogVisitorRecord.noMore();
    }

}
