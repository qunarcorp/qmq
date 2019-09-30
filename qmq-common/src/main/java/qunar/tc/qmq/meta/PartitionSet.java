package qunar.tc.qmq.meta;

import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class PartitionSet {

    private String subject;
    private Set<Integer> physicalPartitions;
    private int version;

    public String getSubject() {
        return subject;
    }

    public PartitionSet setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public Set<Integer> getPhysicalPartitions() {
        return physicalPartitions;
    }

    public PartitionSet setPhysicalPartitions(Set<Integer> physicalPartitions) {
        this.physicalPartitions = physicalPartitions;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public PartitionSet setVersion(int version) {
        this.version = version;
        return this;
    }
}
