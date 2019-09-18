package qunar.tc.qmq.meta;

import com.google.common.collect.Range;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class Partition {

    public enum Status {
        RW, R, W, NRW
    }

    private String subject;
    private String partitionName;
    private int partitionId;
    private Range<Integer> logicalPartition;
    private String brokerGroup;
    private Status status;

    public Partition(String subject, String partitionName, int partitionId, Range<Integer> logicalPartition, String brokerGroup, Status status) {
        this.subject = subject;
        this.partitionName = partitionName;
        this.partitionId = partitionId;
        this.logicalPartition = logicalPartition;
        this.brokerGroup = brokerGroup;
        this.status = status;
    }

    public String getSubject() {
        return subject;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Range<Integer> getLogicalPartition() {
        return logicalPartition;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    public Status getStatus() {
        return status;
    }
}
