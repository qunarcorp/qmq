package qunar.tc.qmq.store;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Created by zhaohui.yu
 * 3/13/19
 */
public class MessageQueryIndex implements Comparable<MessageQueryIndex> {
    private final String subject;

    private final String messageId;

    private final long createTime;

    private final long sequence;

    @JsonIgnore
    private transient int backupRetryTimes = 0;

    @JsonIgnore
    private transient long currentOffset;

    MessageQueryIndex(String subject, String messageId, long sequence, long createTime) {
        this.subject = subject;
        this.messageId = messageId;
        this.sequence = sequence;
        this.createTime = createTime;
    }

    public String getSubject() {
        return subject;
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

    public int getBackupRetryTimes() {
        return backupRetryTimes;
    }

    public void setBackupRetryTimes(int backupRetryTimes) {
        this.backupRetryTimes = backupRetryTimes;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public int compareTo(MessageQueryIndex o) {
        return Long.compare(currentOffset, o.getCurrentOffset());
    }

    @Override
    public int hashCode() {
        return Long.hashCode(currentOffset);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MessageQueryIndex)) return false;
        return currentOffset == ((MessageQueryIndex) obj).currentOffset;
    }
}
