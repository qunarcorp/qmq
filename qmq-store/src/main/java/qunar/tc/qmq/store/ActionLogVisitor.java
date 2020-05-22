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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.action.ActionEvent;
import qunar.tc.qmq.store.buffer.SegmentBuffer;

import java.nio.ByteBuffer;

import static qunar.tc.qmq.store.ActionLog.*;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class ActionLogVisitor extends AbstractLogVisitor<ActionEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ActionLogVisitor.class);

    private static final int MIN_RECORD_BYTES = 5; // 4 bytes magic + 1 byte record type

    public ActionLogVisitor(final LogManager logManager, final long startOffset) {
        super(logManager, startOffset);
    }

    @Override
    protected LogVisitorRecord<ActionEvent> readOneRecord(final SegmentBuffer segmentBuffer) {
        ByteBuffer buffer = segmentBuffer.getBuffer();
        if (buffer.remaining() < MIN_RECORD_BYTES) {
            return LogVisitorRecord.noMore();
        }

        final int startPos = buffer.position();
        final int magic = buffer.getInt();
        if (magic != MagicCode.ACTION_LOG_MAGIC_V1) {
            setVisitedBufferSize(getBufferSize());
            return LogVisitorRecord.noMore();
        }

        final byte attributes = buffer.get();
        if (attributes == ATTR_BLANK_RECORD) {
            if (buffer.remaining() < Integer.BYTES) {
                return LogVisitorRecord.noMore();
            }
            final int blankSize = buffer.getInt();
            incrVisitedBufferSize(blankSize + (buffer.position() - startPos));
            return LogVisitorRecord.empty();
        } else if (attributes == ATTR_EMPTY_RECORD) {
            setVisitedBufferSize(getBufferSize());
            return LogVisitorRecord.noMore();
        } else if (attributes == ATTR_ACTION_RECORD) {
            try {
                if (buffer.remaining() < Integer.BYTES + Byte.BYTES) {
                    return LogVisitorRecord.noMore();
                }
                final ActionType payloadType = ActionType.fromCode(buffer.get());
                final int payloadSize = buffer.getInt();
                if (buffer.remaining() < payloadSize) {
                    return LogVisitorRecord.noMore();
                }
                final Action action = payloadType.getReaderWriter().read(buffer);
                incrVisitedBufferSize(buffer.position() - startPos);

                return LogVisitorRecord.data(new ActionEvent(getStartOffset() + visitedBufferSize(), action));
            } catch (Exception e) {
                LOG.error("fail read action log", e);
                return LogVisitorRecord.noMore();
            }
        } else {
            throw new RuntimeException("Unknown record type " + attributes);
        }
    }
}
