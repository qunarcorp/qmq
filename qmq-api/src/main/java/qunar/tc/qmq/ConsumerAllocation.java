package qunar.tc.qmq;

import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ConsumerAllocation implements Versionable {

    private int allocationVersion;

    private Set<Integer> physicalPartitions;

    public ConsumerAllocation(int allocationVersion, Set<Integer> physicalPartitions) {
        this.allocationVersion = allocationVersion;
        this.physicalPartitions = physicalPartitions;
    }

    @Override
    public int getVersion() {
        return allocationVersion;
    }

    public Set<Integer> getPhysicalPartitions() {
        return physicalPartitions;
    }
}
