package qunar.tc.qmq;

import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ProducerMapping {

    private int mappingVersion;
    private Set<Integer> physicalPartitions;

    public ProducerMapping(int mappingVersion, Set<Integer> physicalPartitions) {
        this.mappingVersion = mappingVersion;
        this.physicalPartitions = physicalPartitions;
    }

    public int getMappingVersion() {
        return mappingVersion;
    }

    public ProducerMapping setMappingVersion(int mappingVersion) {
        this.mappingVersion = mappingVersion;
        return this;
    }

    public Set<Integer> getPhysicalPartitions() {
        return physicalPartitions;
    }

    public ProducerMapping setPhysicalPartitions(Set<Integer> physicalPartitions) {
        this.physicalPartitions = physicalPartitions;
        return this;
    }
}
