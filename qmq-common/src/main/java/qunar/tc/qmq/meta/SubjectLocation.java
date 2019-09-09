package qunar.tc.qmq.meta;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class SubjectLocation {

    private String subjectSuffix;
    private String brokerGroup;

    public SubjectLocation(String subjectSuffix, String brokerGroup) {
        this.subjectSuffix = subjectSuffix;
        this.brokerGroup = brokerGroup;
    }

    public String getSubjectSuffix() {
        return subjectSuffix;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }
}
