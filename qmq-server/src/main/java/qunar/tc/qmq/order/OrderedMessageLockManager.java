package qunar.tc.qmq.order;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public interface OrderedMessageLockManager {

    boolean acquireLock(String partitionName, String group, String clientId, int version);

    boolean releaseLock(String partitionName, String group, String clientId);
}
