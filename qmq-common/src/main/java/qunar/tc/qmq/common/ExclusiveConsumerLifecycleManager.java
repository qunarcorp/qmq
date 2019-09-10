package qunar.tc.qmq.common;

/**
 * 用于 meta server 控制 exclusive consumer 生命周期, 每次心跳后刷新生命周期
 *
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public interface ExclusiveConsumerLifecycleManager {

    boolean isAlive(String subject, String consumerGroup, String brokerGroup, String partitionName);

    boolean refreshLifecycle(String subject, String consumerGroup, String brokerGroup, String partitionName, int version, long expired);
}
