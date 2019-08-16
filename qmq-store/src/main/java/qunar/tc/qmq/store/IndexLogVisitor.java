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

import qunar.tc.qmq.store.buffer.SegmentBuffer;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class IndexLogVisitor extends AbstractLogVisitor<MessageQueryIndex> {

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

        MessageQueryIndex index = new MessageQueryIndex(subject, messageId, sequence, createTime);
        incrVisitedBufferSize(buffer.position() - startPos);
        index.setCurrentOffset(getStartOffset() + visitedBufferSize());
        return LogVisitorRecord.data(index);
    }

    private LogVisitorRecord<MessageQueryIndex> retNoMore() {
        setVisitedBufferSize(getBufferSize());
        return LogVisitorRecord.noMore();
    }

}
