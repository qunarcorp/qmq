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
import java.util.Objects;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public class RangeAck2ActionReaderWriter implements ActionReaderWriter {
    private static final byte TRUE_BYTE = (byte) 1;
    private static final byte FALSE_BYTE = (byte) 0;

    @Override
    public int write(ByteBuffer to, Action action) {
        final int startIndex = to.position();

        final RangeAckAction rangeAck = (RangeAckAction) action;
        PayloadHolderUtils.writeString(rangeAck.partitionName(), to);
        PayloadHolderUtils.writeString(rangeAck.consumerGroup(), to);
        PayloadHolderUtils.writeString(rangeAck.consumerId(), to);

        to.putLong(rangeAck.timestamp());
        to.put(toByte(rangeAck.isExclusiveConsume()));

        to.putLong(rangeAck.getFirstSequence());
        to.putLong(rangeAck.getLastSequence());
        return to.position() - startIndex;
    }

    @Override
    public RangeAckAction read(final ByteBuffer from) {
        final String partitionName = PayloadHolderUtils.readString(from);
        final String consumerGroup = PayloadHolderUtils.readString(from);
        final String consumerId = PayloadHolderUtils.readString(from);
        final long timestamp = from.getLong();
        final boolean isExclusiveConsume = fromByte(from.get());

        final long firstSequence = from.getLong();
        final long lastSequence = from.getLong();

        return new RangeAckAction(partitionName, consumerGroup, consumerId, timestamp, isExclusiveConsume, firstSequence, lastSequence);
    }

    private byte toByte(final boolean bool) {
        return bool ? TRUE_BYTE : FALSE_BYTE;
    }

    private boolean fromByte(final byte b) {
        return Objects.equals(b, TRUE_BYTE);
    }
}
