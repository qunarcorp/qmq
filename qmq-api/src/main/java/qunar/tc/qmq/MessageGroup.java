package qunar.tc.qmq;

import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-06
 */
public class MessageGroup {

    private ClientType clientType;
    private String subject;
    // 主题后缀, 由 meta server 决定, 旧版本的客户端
    private String subjectSuffix;
    private String brokerGroup;

    public MessageGroup(ClientType clientType, String subject, String subjectSuffix, String brokerGroup) {
        this.clientType = clientType;
        this.subject = subject;
        this.subjectSuffix = subjectSuffix;
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

    public MessageGroup setSubjectSuffix(String subjectSuffix) {
        this.subjectSuffix = subjectSuffix;
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

    public String getSubjectSuffix() {
        return subjectSuffix;
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
                Objects.equals(subjectSuffix, that.subjectSuffix) &&
                Objects.equals(brokerGroup, that.brokerGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientType, subject, subjectSuffix, brokerGroup);
    }

    @Override
    public String toString() {
        return clientType + ":" + subject + ":" + subjectSuffix + ":" + brokerGroup;
    }
}
