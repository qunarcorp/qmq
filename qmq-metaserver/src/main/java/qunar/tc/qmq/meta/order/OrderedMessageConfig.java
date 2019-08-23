package qunar.tc.qmq.meta.order;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class OrderedMessageConfig {

    private static final int DEFAULT_LOGICAL_PARTITION_NUM = 16384;
    private static final int DEFAULT_PHYSICAL_PARTITION_NUM = 8;

    public static int getDefaultLogicalPartitionNum() {
        return DEFAULT_LOGICAL_PARTITION_NUM;
    }

    public static int getDefaultPhysicalPartitionNum() {
        return DEFAULT_PHYSICAL_PARTITION_NUM;
    }

    public static List<String> getBrokerGroups() {
        // TODO(zhenwei.liu)
        return null;
    }

    public static List<String> getDelayBrokerGroups() {
        // TODO(zhenwei.liu)
        return null;
    }
}
