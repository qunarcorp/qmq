package qunar.tc.qmq;

import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ConsumerAllocation implements Versionable {

    private int allocationVersion;
    private Set<Integer> physicalPartitions;
    private long expired; // 本次心跳授权超时时间

    public ConsumerAllocation(int allocationVersion, Set<Integer> physicalPartitions, long expired) {
        this.allocationVersion = allocationVersion;
        this.physicalPartitions = physicalPartitions;
        this.expired = expired;
    }

    @Override
    public int getVersion() {
        return allocationVersion;
    }

    public Set<Integer> getPhysicalPartitions() {
        return physicalPartitions;
    }

    public long getExpired() {
        return expired;
    }

    public ConsumerAllocation setExpired(long expired) {
        this.expired = expired;
        return this;
    }
}
