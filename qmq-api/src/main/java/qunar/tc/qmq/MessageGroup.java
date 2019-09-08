package qunar.tc.qmq;

import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-06
 */
public class MessageGroup {

    private ClientType clientType;
    private String subject;
    private int partition;
    private String brokerGroup;

    public MessageGroup(ClientType clientType, String subject, int partition, String brokerGroup) {
        this.clientType = clientType;
        this.subject = subject;
        this.partition = partition;
        this.brokerGroup = brokerGroup;
    }

    public MessageGroup setClientType(ClientType clientType) {
        this.clientType = clientType;
        return this;
    }

    public MessageGroup setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public MessageGroup setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public MessageGroup setBrokerGroup(String brokerGroup) {
        this.brokerGroup = brokerGroup;
        return this;
    }

    public ClientType getClientType() {
        return clientType;
    }

    public String getSubject() {
        return subject;
    }

    public int getPartition() {
        return partition;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageGroup that = (MessageGroup) o;
        return clientType == that.clientType &&
                Objects.equals(subject, that.subject) &&
                Objects.equals(partition, that.partition) &&
                Objects.equals(brokerGroup, that.brokerGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientType, subject, partition, brokerGroup);
    }

    @Override
    public String toString() {
        return clientType + ":" + subject + ":" + partition + ":" + brokerGroup;
    }
}
