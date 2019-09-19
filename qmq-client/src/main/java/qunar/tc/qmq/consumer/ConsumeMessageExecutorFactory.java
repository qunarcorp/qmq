package qunar.tc.qmq.consumer;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.common.ExclusiveConsumerLifecycleManager;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * @author zhenwei.liu
 * @since 2019-09-19
 */
public class ConsumeMessageExecutorFactory {

    public static ConsumeMessageExecutor createExecutor(
            ConsumeStrategy consumeStrategy,
            String subject,
            String consumerGroup,
            String partitionName,
            Executor partitionExecutor,
            MessageHandler messageHandler,
            ExclusiveConsumerLifecycleManager lifecycleManager,
            Executor messageHandleExecutor
    ) {
        if (Objects.equals(ConsumeStrategy.EXCLUSIVE, consumeStrategy)) {
            return new ExclusiveConsumeMessageExecutor(subject, consumerGroup, partitionName, partitionExecutor, messageHandler, lifecycleManager);
        } else if (Objects.equals(ConsumeStrategy.SHARED, consumeStrategy)) {
            return new SharedConsumeMessageExecutor(subject, consumerGroup, partitionName, partitionExecutor, messageHandler, messageHandleExecutor);
        } else {
            throw new IllegalArgumentException(String.format("不支持的 consumeStrategy %s", consumeStrategy));
        }
    }
}
