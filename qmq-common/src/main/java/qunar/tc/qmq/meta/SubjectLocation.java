package qunar.tc.qmq.meta;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class SubjectLocation {

    private String partitionName;
    private String brokerGroup;

    public SubjectLocation(String partitionName, String brokerGroup) {
        this.partitionName = partitionName;
        this.brokerGroup = brokerGroup;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }
}
