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

package qunar.tc.qmq.store.action;

import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionReaderWriter;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public class RangeAckActionReaderWriter implements ActionReaderWriter {
    @Override
    public int write(ByteBuffer to, Action action) {
        final int startIndex = to.position();

        final RangeAckAction rangeAck = (RangeAckAction) action;
        PayloadHolderUtils.writeString(rangeAck.subject(), to);
        PayloadHolderUtils.writeString(rangeAck.group(), to);
        PayloadHolderUtils.writeString(rangeAck.consumerId(), to);
        to.putLong(action.timestamp());
        to.putLong(rangeAck.getFirstSequence());
        to.putLong(rangeAck.getLastSequence());
        return to.position() - startIndex;
    }

    @Override
    public RangeAckAction read(final ByteBuffer from) {
        final String subject = PayloadHolderUtils.readString(from);
        final String group = PayloadHolderUtils.readString(from);
        final String consumerId = PayloadHolderUtils.readString(from);
        final long timestamp = from.getLong();
        final long firstSequence = from.getLong();
        final long lastSequence = from.getLong();

        return new RangeAckAction(subject, group, consumerId, timestamp, firstSequence, lastSequence);
    }
}
