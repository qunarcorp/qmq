package qunar.tc.qmq.meta;

import com.google.common.collect.Lists;
import qunar.tc.qmq.PartitionProps;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class PartitionPropsUtils {

    public static List<PartitionProps> getPartitionPropsByBrokerGroup(String brokerGroup, Collection<PartitionProps> partitionProps) {
        List<PartitionProps> result = Lists.newArrayList();
        for (PartitionProps partitionProp : partitionProps) {
            if (Objects.equals(partitionProp.getBrokerGroup(), brokerGroup)) {
                result.add(partitionProp);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException(String.format("无法找到 brokerGroup %s 对应的 partition", brokerGroup));
        }
        return result;
    }
}
