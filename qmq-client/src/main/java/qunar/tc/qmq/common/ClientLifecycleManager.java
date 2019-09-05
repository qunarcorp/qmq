package qunar.tc.qmq.common;

import java.util.Date;

/**
 * 用于 meta server 控制 client 声明周期, 每次心跳后刷新生命周期
 *
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public interface ClientLifecycleManager {

    boolean isAlive(String subject, String group, int physicalPartition);

    boolean refreshLifecycle(String subject, String group, int physicalPartition, int version, long expired);
}
