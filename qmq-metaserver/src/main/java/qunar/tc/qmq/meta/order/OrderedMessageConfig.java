package qunar.tc.qmq.meta.order;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class OrderedMessageConfig {

    public static int getDefaultLogicalPartitionNum() {
        // TODO
        return 0;
    }

    public static int getDefaultPhysicalPartitionNum() {
        // TODO
        return 0;
    }

    public static List<String> getBrokerGroups() {
        // TODO
        return null;
    }

    public static List<String> getDelayBrokerGroups() {
        // TODO
        return null;
    }
}
