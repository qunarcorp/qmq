package qunar.tc.qmq.order;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public interface ExclusiveMessageLockManager {

    boolean acquireLock(String partitionName, String consumerGroup, String clientId);

    boolean releaseLock(String partitionName, String consumerGroup, String clientId);
}
