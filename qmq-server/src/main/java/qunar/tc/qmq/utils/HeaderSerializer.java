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

import qunar.tc.qmq.protocol.RemotingHeader;

import java.nio.ByteBuffer;

import static qunar.tc.qmq.protocol.RemotingHeader.*;

public class HeaderSerializer {
    public static ByteBuffer serialize(RemotingHeader header, int payloadSize, int additional) {
        short headerSize = MIN_HEADER_SIZE;
        int bufferLength = TOTAL_SIZE_LEN + HEADER_SIZE_LEN + headerSize + additional;
        ByteBuffer buffer = ByteBuffer.allocate(bufferLength);
        // total len
        int total = HEADER_SIZE_LEN + headerSize + payloadSize;
        buffer.putInt(total);
        // header size
        buffer.putShort(headerSize);
        // magic code
        buffer.putInt(header.getMagicCode());
        // code
        buffer.putShort(header.getCode());
        // version
        buffer.putShort(header.getVersion());
        // opaque
        buffer.putInt(header.getOpaque());
        // flag
        buffer.putInt(header.getFlag());
        buffer.putShort(header.getRequestCode());
        return buffer;
    }
}
