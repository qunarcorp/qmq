package qunar.tc.qmq.protocol;

/**
 * @author zhenwei.liu
 * @since 2019-09-24
 */
public class QuerySubjectResponse {

    private String subject;
    private String partitionName;

    public QuerySubjectResponse(String subject, String partitionName) {
        this.subject = subject;
        this.partitionName = partitionName;
    }

    public String getSubject() {
        return subject;
    }

    public String getPartitionName() {
        return partitionName;
    }
}
