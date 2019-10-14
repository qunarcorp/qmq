package qunar.tc.qmq.order;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public interface ExclusiveConsumerLockManager {

    // TODO(zhenwei.liu) 只要存在独占, 就只能由独占消费, 其他 Shared 的都不能消费
    boolean acquireLock(String partitionName, String consumerGroup, String clientId);

    boolean refreshLock(String partitionName, String consumerGroup, String clientId);

    boolean releaseLock(String partitionName, String consumerGroup, String clientId);
}
