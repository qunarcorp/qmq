package qunar.tc.qmq.common;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public class PartitionConstants {

    public static final int DEFAULT_LOGICAL_PARTITION_NUM = 1024;
    public static final int DEFAULT_PHYSICAL_PARTITION_NUM = 8;
    public static final long ORDERED_CLIENT_HEARTBEAT_INTERVAL_SECS = 3;
    public static final long ORDERED_CONSUMER_LOCK_LEASE_SECS = ORDERED_CLIENT_HEARTBEAT_INTERVAL_SECS * 3;

    public static final String EMPTY_SUBJECT_SUFFIX = "";
}
