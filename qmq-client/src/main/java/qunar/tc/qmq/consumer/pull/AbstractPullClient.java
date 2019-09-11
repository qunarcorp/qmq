package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.PullClient;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public abstract class AbstractPullClient implements PullClient {

    private String subject;
    private String consumerGroup;
    private String partitionName;
    private String brokerGroup;
    private int version;

    public AbstractPullClient(String subject, String consumerGroup, String brokerGroup, String partitionName, int version) {
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.brokerGroup = brokerGroup;
        this.partitionName = partitionName;
        this.version = version;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public String getBrokerGroup() {
        return brokerGroup;
    }

    @Override
    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }
}
