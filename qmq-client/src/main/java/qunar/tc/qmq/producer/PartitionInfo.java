package qunar.tc.qmq.producer;

import com.google.common.collect.RangeMap;
import qunar.tc.qmq.meta.BrokerGroup;

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
    private RangeMap<Integer, Integer> logicalPartitionMap; // 逻辑分区与物理分区的映射
    private Map<Integer, BrokerGroup> physicalPartitionMap; // 物理分区与 broker 的映射
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

    public RangeMap<Integer, Integer> getLogicalPartitionMap() {
        return logicalPartitionMap;
    }

    public PartitionInfo setLogicalPartitionMap(
            RangeMap<Integer, Integer> logicalPartitionMap) {
        this.logicalPartitionMap = logicalPartitionMap;
        return this;
    }

    public Map<Integer, BrokerGroup> getPhysicalPartitionMap() {
        return physicalPartitionMap;
    }

    public PartitionInfo setPhysicalPartitionMap(
            Map<Integer, BrokerGroup> physicalPartitionMap) {
        this.physicalPartitionMap = physicalPartitionMap;
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
