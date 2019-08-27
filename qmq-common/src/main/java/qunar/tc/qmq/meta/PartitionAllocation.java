package qunar.tc.qmq.meta;

import java.util.Map;
import java.util.Set;

/**
 * 代表一个版本的分区信息, Consumer 使用
 *
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public class PartitionAllocation {

    private String subject;
    private String consumerGroup;
    private AllocationDetail allocationDetail;
    private int partitionSetVersion;
    private int version;

    public String getSubject() {
        return subject;
    }

    public PartitionAllocation setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public PartitionAllocation setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public AllocationDetail getAllocationDetail() {
        return allocationDetail;
    }

    public PartitionAllocation setAllocationDetail(AllocationDetail allocationDetail) {
        this.allocationDetail = allocationDetail;
        return this;
    }

    public int getPartitionSetVersion() {
        return partitionSetVersion;
    }

    public PartitionAllocation setPartitionSetVersion(int partitionSetVersion) {
        this.partitionSetVersion = partitionSetVersion;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public PartitionAllocation setVersion(int version) {
        this.version = version;
        return this;
    }

    public static class AllocationDetail {

        private Map<String, Set<Integer>> clientId2PhysicalPartitions;

        public Map<String, Set<Integer>> getClientId2PhysicalPartitions() {
            return clientId2PhysicalPartitions;
        }

        public AllocationDetail setClientId2PhysicalPartitions(Map<String, Set<Integer>> clientId2PhysicalPartitions) {
            this.clientId2PhysicalPartitions = clientId2PhysicalPartitions;
            return this;
        }
    }
}
