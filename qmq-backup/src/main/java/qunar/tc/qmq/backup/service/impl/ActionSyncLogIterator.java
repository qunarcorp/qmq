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

package qunar.tc.qmq.backup.service.impl;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.backup.base.QmqBackupException;
import qunar.tc.qmq.backup.service.SyncLogIterator;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionType;
import qunar.tc.qmq.store.LogVisitorRecord;
import qunar.tc.qmq.store.MagicCode;

import static qunar.tc.qmq.store.ActionLog.*;

public class ActionSyncLogIterator implements SyncLogIterator<Action, ByteBuf> {

    public static final Action BLANK_ACTION = new Action() {
        @Override
        public ActionType type() {
            throw new QmqBackupException("Blank Action");
        }

        @Override
        public String subject() {
            throw new QmqBackupException("Blank Action");
        }

        @Override
        public String group() {
            throw new QmqBackupException("Blank Action");
        }

        @Override
        public String consumerId() {
            throw new QmqBackupException("Blank Action");
        }

        @Override
        public long timestamp() {
            throw new QmqBackupException("Blank Action");
        }
    };

    @Override
    public boolean hasNext(ByteBuf buf) {
        return buf.isReadable(MIN_RECORD_BYTES);
    }

    @Override
    public LogVisitorRecord<Action> next(ByteBuf buf) {
        final int magic = buf.readInt();
        if (magic != MagicCode.ACTION_LOG_MAGIC_V1) {
            return LogVisitorRecord.noMore();
        }

        final byte attributes = buf.readByte();
        if (attributes == ATTR_BLANK_RECORD) {
            if (buf.readableBytes() < Integer.BYTES) {
                return LogVisitorRecord.noMore();
            }
            final int blankSize = buf.readInt();
            buf.readerIndex(buf.readerIndex() + blankSize);
            return LogVisitorRecord.data(BLANK_ACTION);
        } else if (attributes == ATTR_EMPTY_RECORD) {
            buf.readerIndex(buf.readerIndex() + buf.readableBytes());
            return LogVisitorRecord.empty();
        } else if (attributes == ATTR_ACTION_RECORD) {
            if (buf.readableBytes() < Integer.BYTES + Byte.BYTES) {
                return LogVisitorRecord.noMore();
            }
            final ActionType payloadType = ActionType.fromCode(buf.readByte());
            final int payloadSize = buf.readInt();
            if (buf.readableBytes() < payloadSize) {
                return LogVisitorRecord.noMore();
            }

            if (buf.nioBufferCount() > 0) {
                final int payloadIndex = buf.readerIndex();
                final Action action = payloadType.getReaderWriter().read(buf.nioBuffer(payloadIndex, payloadSize));
                buf.readerIndex(buf.readerIndex() + payloadSize);
                return LogVisitorRecord.data(action);
            } else {
                return LogVisitorRecord.data(BLANK_ACTION);
            }
        } else {
            throw new RuntimeException("Unknown record type");
        }
    }
}
