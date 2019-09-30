package qunar.tc.qmq.common;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public class PartitionConstants {

    public static final int DEFAULT_LOGICAL_PARTITION_NUM = 1024;
    public static final int DEFAULT_PHYSICAL_PARTITION_NUM = 8;
    public static final long EXCLUSIVE_CLIENT_HEARTBEAT_INTERVAL_MILLS = 3000;
    public static final long EXCLUSIVE_CONSUMER_LOCK_LEASE_MILLS = EXCLUSIVE_CLIENT_HEARTBEAT_INTERVAL_MILLS * 3;
    public static final int EMPTY_VERSION = -1;
    public static final int EMPTY_PARTITION_ID = -1;
}
