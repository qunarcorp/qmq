package qunar.tc.qmq.meta;

import com.google.common.collect.RangeMap;

import java.util.Map;

/**
 * 代表一个版本的分区信息
 *
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public class PartitionInfo {

    private String subject;
    private int logicalPartitionNum;
    private RangeMap<Integer, Integer> logical2PhysicalPartition; // 逻辑分区与物理分区的映射
    private RangeMap<Integer, String> physicalPartition2Broker; // 物理分区与 broker 的映射
    private RangeMap<Integer, String> physicalPartition2DelayBroker; // 物理分区与 delay broker 的映射
    private int version;

    public String getSubject() {
        return subject;
    }

    public PartitionInfo setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public int getLogicalPartitionNum() {
        return logicalPartitionNum;
    }

    public PartitionInfo setLogicalPartitionNum(int logicalPartitionNum) {
        this.logicalPartitionNum = logicalPartitionNum;
        return this;
    }

    public RangeMap<Integer, Integer> getLogical2PhysicalPartition() {
        return logical2PhysicalPartition;
    }

    public PartitionInfo setLogical2PhysicalPartition(
            RangeMap<Integer, Integer> logical2PhysicalPartition) {
        this.logical2PhysicalPartition = logical2PhysicalPartition;
        return this;
    }

    public RangeMap<Integer, String> getPhysicalPartition2Broker() {
        return physicalPartition2Broker;
    }

    public PartitionInfo setPhysicalPartition2Broker(RangeMap<Integer, String> physicalPartition2Broker) {
        this.physicalPartition2Broker = physicalPartition2Broker;
        return this;
    }

    public RangeMap<Integer, String> getPhysicalPartition2DelayBroker() {
        return physicalPartition2DelayBroker;
    }

    public PartitionInfo setPhysicalPartition2DelayBroker(RangeMap<Integer, String> physicalPartition2DelayBroker) {
        this.physicalPartition2DelayBroker = physicalPartition2DelayBroker;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public PartitionInfo setVersion(int version) {
        this.version = version;
        return this;
    }
}
