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

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.utils.Crc32;
import qunar.tc.qmq.utils.DelayUtil;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhaohui.yu
 * 7/21/17
 */
public class MessagesPayloadHolder implements PayloadHolder {
    private final List<BaseMessage> messages;

    public MessagesPayloadHolder(List<BaseMessage> messages) {
        this.messages = messages;
    }

    @Override
    public void writeBody(ByteBuf out) {
        if (messages == null || messages.size() == 0) return;
        for (BaseMessage message : messages) {
            serializeMessage(message, out);
        }
    }

    private void serializeMessage(BaseMessage message, ByteBuf out) {
        int crcIndex = out.writerIndex();
        // sizeof(bodyCrc<long>)
        out.ensureWritable(8);
        out.writerIndex(crcIndex + 8);

        final int messageStart = out.writerIndex();

        // flag
        byte flag = 0;
        //由低到高，第二位标识延迟(1)非延迟(0)，第三位标识是(1)否(0)包含Tag
        flag = Flags.setDelay(flag, DelayUtil.isDelayMessage(message));

        //in avoid add tag after sendMessage
        Set<String> tags = new HashSet<>(message.getTags());
        flag = Flags.setTags(flag, hasTags(tags));

        out.writeByte(flag);

        // created time
        out.writeLong(message.getCreatedTime().getTime());
        if (Flags.isDelay(flag)) {
            out.writeLong(message.getScheduleReceiveTime().getTime());
        } else {
            // expired time
            out.writeLong(System.currentTimeMillis());
        }
        // subject
        writeSubject(message, out);
        // message id
        PayloadHolderUtils.writeString(message.getMessageId(), out);

        writeTags(tags, out);

        out.markWriterIndex();
        // writerIndex + sizeof(bodyLength<int>)
        final int bodyStart = out.writerIndex() + 4;
        out.ensureWritable(4);
        out.writerIndex(bodyStart);

        serializeMap(message.getAttrs(), out);
        final int bodyEnd = out.writerIndex();

        final int messageEnd = out.writerIndex();

        final int bodyLen = bodyEnd - bodyStart;
        final int messageLength = bodyEnd - messageStart;

        // write body length
        out.resetWriterIndex();
        out.writeInt(bodyLen);

        // write message crc
        out.writerIndex(crcIndex);
        out.writeLong(messageCrc(out, messageStart, messageLength));

        out.writerIndex(messageEnd);
    }

    private void writeTags(Set<String> tags, ByteBuf out) {
        if (tags.isEmpty()) return;
        out.writeByte((byte) tags.size());
        for (final String tag : tags) {
            PayloadHolderUtils.writeString(tag, out);
        }
    }

    private boolean hasTags(Set<String> tags) {
        return tags.size() > 0;
    }

    //TODO: 这里应该针对超大的消息记录监控
    private void serializeMap(Map<String, Object> map, ByteBuf out) {
        if (null == map || map.isEmpty()) return;

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) continue;
            PayloadHolderUtils.writeString(entry.getKey(), out);
            PayloadHolderUtils.writeString(entry.getValue().toString(), out);
        }
    }

    private long messageCrc(ByteBuf out, int messageStart, int messageLength) {
        return Crc32.crc32(out.nioBuffer(messageStart, messageLength), 0, messageLength);
    }

    protected void writeSubject(BaseMessage message, ByteBuf out) {
        PayloadHolderUtils.writeString(message.getPartitionName(), out);
    }
}
