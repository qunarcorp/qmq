package qunar.tc.qmq.common;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ClientLifecycleManagerFactory {

    private static final ExclusiveConsumerLifecycleManager instance = new DefaultExclusiveConsumerLifecycleManager();

    public static ExclusiveConsumerLifecycleManager get() {
        return instance;
    }
}
