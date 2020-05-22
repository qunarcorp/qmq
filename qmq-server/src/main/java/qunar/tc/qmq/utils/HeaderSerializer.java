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

package qunar.tc.qmq.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import qunar.tc.qmq.protocol.RemotingHeader;

import static qunar.tc.qmq.protocol.RemotingHeader.*;

public class HeaderSerializer {
    public static ByteBuf serialize(RemotingHeader header, int payloadSize, int additional) {
        short headerSize = MIN_HEADER_SIZE;
        int bufferLength = TOTAL_SIZE_LEN + HEADER_SIZE_LEN + headerSize + additional;
        ByteBuf buffer = ByteBufAllocator.DEFAULT.ioBuffer(bufferLength);
        // total len
        int total = HEADER_SIZE_LEN + headerSize + payloadSize;
        buffer.writeInt(total);
        // header size
        buffer.writeShort(headerSize);
        // magic code
        buffer.writeInt(header.getMagicCode());
        // code
        buffer.writeShort(header.getCode());
        // version
        buffer.writeShort(header.getVersion());
        // opaque
        buffer.writeInt(header.getOpaque());
        // flag
        buffer.writeInt(header.getFlag());
        buffer.writeShort(header.getRequestCode());
        return buffer;
    }
}
