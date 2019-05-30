package qunar.tc.qmq.base;

import java.io.Serializable;
import java.util.List;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-20 15:40
 */
public class RemoteMessageQuery implements Serializable {
    private static final long serialVersionUID = 8856153577326864895L;

    private String subject;
    private List<MessageKey> keys;

    public RemoteMessageQuery(String subject, List<MessageKey> keys) {
        this.subject = subject;
        this.keys = keys;
    }

    public String getSubject() {
        return subject;
    }

    public List<MessageKey> getKeys() {
        return keys;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setKeys(List<MessageKey> keys) {
        this.keys = keys;
    }

    public static class MessageKey {
        private long sequence;

        public MessageKey(long sequence) {
            this.sequence = sequence;
        }

        public void setSequence(long sequence) {
            this.sequence = sequence;
        }

        public long getSequence() {
            return sequence;
        }

    }
}
