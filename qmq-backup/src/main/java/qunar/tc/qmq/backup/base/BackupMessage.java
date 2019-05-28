package qunar.tc.qmq.backup.base;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
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
     * 消息发送记录
     */
    private String records;
    /**
     * 备份时间
     */
    private long timestamp;

    private byte action;

    private long sequence;

    private byte flag;

    private String consumerGroup;

    private String consumerId;

    private String brokerGroup;

    @JsonIgnore
    private int backupRetryTimes = 0;

    private BaseMessage message;

    public BackupMessage() {
        super();
    }

    public BackupMessage(String messageId, String subject) {
        super(messageId, subject);
    }

    public BackupMessage(BaseMessage message) {
        super(message);
        this.message = message;
    }

    public BackupMessage(BaseMessage message, String records) {
        super(message);
        this.message = message;
        this.records = records;
    }

    public String getRecords() {
        // 存入HBase的值不能为null
        return Strings.nullToEmpty(records);
    }

    public void setRecords(String records) {
        this.records = records;
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

    @JsonIgnore
    public int getBackupRetryTimes() {
        return backupRetryTimes;
    }

    @JsonIgnore
    public void setBackupRetryTimes(int backupRetryTimes) {
        this.backupRetryTimes = backupRetryTimes;
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
        if (records != null) {
            sb.append(":{").append(records).append("}");
        }
        return sb.append(":").toString();
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
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
