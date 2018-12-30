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

package qunar.tc.qmq.delay.store.visitor;

import qunar.tc.qmq.delay.ScheduleIndex;
import qunar.tc.qmq.utils.CharsetUtils;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-15 15:01
 */
public class ScheduleIndexVisitor extends AbstractLogVisitor<ScheduleIndex> {

    public ScheduleIndexVisitor(long from, FileChannel fileChannel, int singleMessageLimitSize) {
        super(from, fileChannel, singleMessageLimitSize);
    }

    @Override
    protected Optional<ScheduleIndex> readOneRecord(ByteBuffer buffer) {
        long curPos = buffer.position();

        if (buffer.remaining() < Long.BYTES) {
            return Optional.empty();
        }
        long scheduleTime = buffer.getLong();

        if (buffer.remaining() < Long.BYTES) {
            return Optional.empty();
        }
        long sequence = buffer.getLong();

        if (buffer.remaining() < Integer.BYTES) {
            return Optional.empty();
        }
        int payloadSize = buffer.getInt();

        if (buffer.remaining() < Integer.BYTES) {
            return Optional.empty();
        }
        int messageId = buffer.getInt();

        if (buffer.remaining() < messageId) {
            return Optional.empty();
        }
        buffer.position(buffer.position() + messageId);

        if (buffer.remaining() < Integer.BYTES) {
            return Optional.empty();
        }
        int subjectLen = buffer.getInt();

        if (buffer.remaining() < subjectLen) {
            return Optional.empty();
        }
        byte[] bs = new byte[subjectLen];
        buffer.get(bs);
        String subject = CharsetUtils.toUTF8String(bs);

        if (buffer.remaining() < payloadSize) {
            return Optional.empty();
        }

        int metaSize = getMetaSize(messageId, subjectLen);
        int recordSize = metaSize + payloadSize;
        long startOffset = visitedBufferSize();
        buffer.position(Math.toIntExact(curPos + recordSize));

        return Optional.of(new ScheduleIndex(subject, scheduleTime, startOffset, recordSize, sequence));
    }

    private int getMetaSize(int messageId, int subjectLen) {
        return 8 + 8
                + 4
                + 4
                + 4
                + messageId
                + subjectLen;
    }
}
