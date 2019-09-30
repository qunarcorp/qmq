package qunar.tc.qmq.protocol;

/**
 * @author zhenwei.liu
 * @since 2019-09-17
 */
public class QuerySubjectRequest {

    private String partitionName;

    public QuerySubjectRequest(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getPartitionName() {
        return partitionName;
    }
}
