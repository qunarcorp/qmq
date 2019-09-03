package qunar.tc.qmq.meta;

import com.google.common.collect.RangeMap;
import qunar.tc.qmq.Versionable;

/**
 * partition 映射, producer 使用
 *
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class PartitionMapping implements Versionable {

    private String subject;
    private int logicalPartitionNum;
    private RangeMap<Integer, Partition> logical2PhysicalPartition;
    private int version;

    public String getSubject() {
        return subject;
    }

    public PartitionMapping setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public int getLogicalPartitionNum() {
        return logicalPartitionNum;
    }

    public PartitionMapping setLogicalPartitionNum(int logicalPartitionNum) {
        this.logicalPartitionNum = logicalPartitionNum;
        return this;
    }

    public RangeMap<Integer, Partition> getLogical2PhysicalPartition() {
        return logical2PhysicalPartition;
    }

    public PartitionMapping setLogical2PhysicalPartition(RangeMap<Integer, Partition> logical2PhysicalPartition) {
        this.logical2PhysicalPartition = logical2PhysicalPartition;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public PartitionMapping setVersion(int version) {
        this.version = version;
        return this;
    }
}
