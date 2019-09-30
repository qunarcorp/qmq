package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.PullRequestV10;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.store.buffer.MemTableBuffer;
import qunar.tc.qmq.utils.DelayUtil;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-01
 */
public class TestToolBox {

    public static final String DEFAULT_MESSAGE_ID = "default_msg_id";
    public static final String DEFAULT_SUBJECT = "default_subject";
    public static final String DEFAULT_CONSUMER_ID = "default_consuemrId@abc";
    public static final String DEFAULT_GROUP = "default_group";

    public static PullRequest createDefaultPullRequest() {
        return new PullRequestV10(DEFAULT_SUBJECT, DEFAULT_GROUP, -1, -1, -1, -1, -1, DEFAULT_CONSUMER_ID, ConsumeStrategy.SHARED, null, -1);
    }

    public static Buffer createDefaultBuffer() {
        BaseMessage message = createDefaultMessage();
        return messageToBuffer(message);
    }

    public static BaseMessage createDefaultMessage() {
        return new BaseMessage(DEFAULT_MESSAGE_ID, DEFAULT_SUBJECT);
    }

    public static Buffer messageToBuffer(BaseMessage message) {
        ByteBuf out = Unpooled.buffer();
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
        PayloadHolderUtils.writeString(message.getSubject(), out);
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
        out.writerIndex(messageEnd);

        return new MemTableBuffer(out, out.writerIndex());
    }

    private static void writeTags(Set<String> tags, ByteBuf out) {
        if (tags.isEmpty()) {
            return;
        }
        out.writeByte((byte) tags.size());
        for (final String tag : tags) {
            PayloadHolderUtils.writeString(tag, out);
        }
    }

    private static boolean hasTags(Set<String> tags) {
        return tags.size() > 0;
    }

    //TODO: 这里应该针对超大的消息记录监控
    private static void serializeMap(Map<String, Object> map, ByteBuf out) {
        if (null == map || map.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            PayloadHolderUtils.writeString(entry.getKey(), out);
            PayloadHolderUtils.writeString(entry.getValue().toString(), out);
        }
    }
}
