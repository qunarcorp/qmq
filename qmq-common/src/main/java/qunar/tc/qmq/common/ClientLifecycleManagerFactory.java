package qunar.tc.qmq.common;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ClientLifecycleManagerFactory {

    private static final OrderedClientLifecycleManager instance = new DefaultOrderedClientLifecycleManager();

    public static OrderedClientLifecycleManager get() {
        return instance;
    }
}
