package qunar.tc.qmq.order;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public interface ExclusiveConsumerLockManager {

    // TODO(zhenwei.liu) 这里所得是 partition range, 而不是 filename
    boolean acquireLock(String partitionName, String consumerGroup, String clientId);

    boolean refreshLock(String partitionName, String consumerGroup, String clientId);

    boolean releaseLock(String partitionName, String consumerGroup, String clientId);
}
