package qunar.tc.qmq.backup.base;

import qunar.tc.qmq.base.BaseMessage;

/**
 * 备份消息
 *
 * @author kelly.li
 * @date 2014-03-01
 */
public class BackupMessage extends BaseMessage {
    private static final long serialVersionUID = -1432424593840809026L;

    /**
     * 备份时间
     */
    private transient long timestamp;

    private byte action;

    private long sequence;

    private String consumerGroup;

    private String consumerId;

    private String brokerGroup;

    private BaseMessage message;

    public BackupMessage() {
        super();
    }

    public BackupMessage(String messageId, String subject) {
        super(messageId, subject);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte getAction() {
        return action;
    }

    public void setAction(byte action) {
        this.action = action;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public BaseMessage getMessage() {
        return message;
    }

    public void setMessage(BaseMessage message) {
        this.message = message;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (message == null) {
            sb.append(getSubject()).append(":").append(getMessageId());
        } else {
            sb.append(message.getSubject()).append(":").append(message.getMessageId());
        }
        return sb.append(":").toString();
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    public void setBrokerGroup(String brokerGroup) {
        this.brokerGroup = brokerGroup;
    }
}
