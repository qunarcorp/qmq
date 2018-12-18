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

package qunar.tc.qmq.protocol;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.protocol.producer.SendResult;
import qunar.tc.qmq.utils.Crc32;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author yunfeng.yang
 * @since 2017/7/14
 */
public class QMQSerializer {

    public static RawMessage deserializeRawMessage(ByteBuf body) {
        int headerStart = body.readerIndex();
        body.markReaderIndex();
        MessageHeader header = deserializeMessageHeader(body);
        int bodyLen = body.readInt();
        int headerLen = body.readerIndex() - headerStart;

        int totalLen = headerLen + bodyLen;
        body.resetReaderIndex();
        byte[] data = new byte[totalLen];
        body.readBytes(data);
        header.setBodyCrc(Crc32.crc32(data));
        return new RawMessage(header, Unpooled.wrappedBuffer(data), totalLen);
    }

    public static MessageHeader deserializeMessageHeader(ByteBuf body) {
        byte flag = body.readByte();
        long createdTime = body.readLong();
        long expiredTime = body.readLong();
        String subject = PayloadHolderUtils.readString(body);
        String messageId = PayloadHolderUtils.readString(body);
        MessageHeader header = new MessageHeader();
        if (Flags.hasTags(flag)) {
            final Set<String> tags = new HashSet<>();
            final byte tagsSize = body.readByte();
            for (int i = 0; i < tagsSize; i++) {
                String tag = PayloadHolderUtils.readString(body);
                tags.add(tag);
            }
            header.setTags(tags);
        }

        header.setFlag(flag);
        header.setCreateTime(createdTime);
        header.setExpireTime(expiredTime);
        header.setSubject(subject);
        header.setMessageId(messageId);
        return header;
    }

    public static Map<String, SendResult> deserializeSendResultMap(ByteBuf buf) {
        Map<String, SendResult> result = Maps.newHashMap();
        while (buf.isReadable()) {
            String messageId = PayloadHolderUtils.readString(buf);
            int code = buf.readInt();
            String remark = PayloadHolderUtils.readString(buf);
            result.put(messageId, new SendResult(code, remark));
        }
        return result;
    }

}
