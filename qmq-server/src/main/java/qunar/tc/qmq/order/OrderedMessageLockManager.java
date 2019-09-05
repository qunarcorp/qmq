package qunar.tc.qmq.order;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public interface OrderedMessageLockManager {

    boolean acquireLock(String subject, String group, String clientId, int version);

    boolean releaseLock(String subject, String group, String clientId);
}
