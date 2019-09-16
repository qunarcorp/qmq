package qunar.tc.qmq.meta;

import qunar.tc.qmq.PartitionProps;

import java.util.Collection;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class PartitionPropsUtils {

    public static PartitionProps getPartitionPropsBrokerGroup(String brokerGroup, Collection<PartitionProps> partitionProps) {
        for (PartitionProps partitionProp : partitionProps) {
            if (Objects.equals(partitionProp.getBrokerGroup(), brokerGroup)) {
                return partitionProp;
            }
        }
        throw new IllegalArgumentException(String.format("无法找到 brokerGroup 对应的 subjectLocation, brokerGroup %s", brokerGroup));

    }
}
