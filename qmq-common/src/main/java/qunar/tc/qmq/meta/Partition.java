package qunar.tc.qmq.meta;

import com.google.common.collect.Range;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class Partition {

    public enum Status {
        RW, R
    }

    private String subject;
    private int physicalPartition;
    private Range<Integer> logicalPartition;
    private String brokerGroup;
    private Status status;

    public String getSubject() {
        return subject;
    }

    public Partition setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public int getPhysicalPartition() {
        return physicalPartition;
    }

    public Partition setPhysicalPartition(int physicalPartition) {
        this.physicalPartition = physicalPartition;
        return this;
    }

    public Range<Integer> getLogicalPartition() {
        return logicalPartition;
    }

    public Partition setLogicalPartition(Range<Integer> logicalPartition) {
        this.logicalPartition = logicalPartition;
        return this;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    public Partition setBrokerGroup(String brokerGroup) {
        this.brokerGroup = brokerGroup;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Partition setStatus(Status status) {
        this.status = status;
        return this;
    }
}
