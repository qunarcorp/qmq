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
    private String subjectSuffix;
    private String brokerGroup;
    private int version;
    private ConsumeMode consumeMode;

    public AbstractPullClient(String subject, String consumerGroup, String brokerGroup, ConsumeMode consumeMode, String subjectSuffix, int version) {
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.brokerGroup = brokerGroup;
        this.subjectSuffix = subjectSuffix;
        this.version = version;
        this.consumeMode = consumeMode;
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
    public String getSubjectSuffix() {
        return subjectSuffix;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public ConsumeMode getConsumeMode() {
        return consumeMode;
    }
}
