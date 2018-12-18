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

package qunar.tc.qmq.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;

/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public class EncodeHandler extends MessageToByteEncoder<Datagram> {

    //total|header size|header|body
    //total = len(header size) + len(header) + len(body)
    //header size = len(header)
    @Override
    protected void encode(ChannelHandlerContext ctx, Datagram msg, ByteBuf out) throws Exception {
        int start = out.writerIndex();
        int headerStart = start + RemotingHeader.LENGTH_FIELD;
        out.ensureWritable(RemotingHeader.LENGTH_FIELD);
        out.writerIndex(headerStart);

        final RemotingHeader header = msg.getHeader();
        encodeHeader(header, out);
        int headerSize = out.writerIndex() - headerStart;

        msg.writeBody(out);
        int end = out.writerIndex();
        int total = end - start - RemotingHeader.TOTAL_SIZE_LEN;

        out.writerIndex(start);
        out.writeInt(total);
        out.writeShort((short) headerSize);
        out.writerIndex(end);
    }

    private static void encodeHeader(final RemotingHeader header, final ByteBuf out) {
        out.writeInt(header.getMagicCode());
        out.writeShort(header.getCode());
        out.writeShort(header.getVersion());
        out.writeInt(header.getOpaque());
        out.writeInt(header.getFlag());
        out.writeShort(header.getRequestCode());
    }
}
