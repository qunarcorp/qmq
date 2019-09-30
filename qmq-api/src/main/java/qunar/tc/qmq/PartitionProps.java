package qunar.tc.qmq;

import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class PartitionProps {

    private int partitionId;
    private String partitionName;
    private String brokerGroup;

    public PartitionProps(int partitionId, String partitionName, String brokerGroup) {
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.brokerGroup = brokerGroup;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionProps that = (PartitionProps) o;
        return Objects.equals(partitionId, that.partitionId) &&
                Objects.equals(partitionName, that.partitionName) &&
                Objects.equals(brokerGroup, that.brokerGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, partitionName, brokerGroup);
    }
}
