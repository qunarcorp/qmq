package qunar.tc.qmq.backup.base;

import com.google.common.collect.Lists;
import qunar.tc.qmq.backup.util.Serializer;

import java.io.Serializable;
import java.util.List;

/**
 * User: zhaohuiyu Date: 3/22/14 Time: 10:29 PM
 */
public class MessageQueryResult implements Serializable {
    private static final long serialVersionUID = -6106414829068194397L;

    private List<MessageMeta> list = Lists.newArrayList();
    private Serializable next;

    public MessageQueryResult() {
        super();
    }

    public void setList(List<MessageMeta> list) {
        this.list = list;
    }

    public List<MessageMeta> getList() {
        return list;
    }

    public void setNext(Serializable next) {
        this.next = next;
    }

    public Serializable getNext() {
        return next;
    }

    public static class MessageMeta {
        private final String subejct;
        private final String messageId;
        private final long sequence;
        private final long createTime;
        private final String brokerGroup;

        public MessageMeta(String subject, String messageId, long sequence, long createTime, String brokerGroup) {
            this.subejct = subject;
            this.messageId = messageId;
            this.sequence = sequence;
            this.createTime = createTime;
            this.brokerGroup = brokerGroup;
        }

        public String getSubejct() {
            return subejct;
        }

        public String getMessageId() {
            return messageId;
        }

        public long getSequence() {
            return sequence;
        }

        public long getCreateTime() {
            return createTime;
        }

        public String getBrokerGroup() {
            return brokerGroup;
        }
    }

    public static void main(String[] args) {
        Serializer serializer = Serializer.getSerializer();
        MessageQueryResult result = new MessageQueryResult();
        List<MessageMeta> messageMetas = Lists.newArrayList();
        MessageMeta meta = new MessageMeta("subject", "msgId", 0, 0, "brokerGroup");
        messageMetas.add(meta);
        result.setList(messageMetas);
        System.out.println(serializer.serialize(result));
    }

}
