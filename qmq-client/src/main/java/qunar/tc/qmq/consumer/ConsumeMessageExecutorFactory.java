package qunar.tc.qmq.consumer;

import qunar.tc.qmq.ConsumeStrategy;

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
            Executor messageHandleExecutor,
            long consumptionExpiredTime
    ) {
        if (Objects.equals(ConsumeStrategy.EXCLUSIVE, consumeStrategy)) {
            return new ExclusiveConsumeMessageExecutor(subject, consumerGroup, partitionName, partitionExecutor, messageHandler, consumptionExpiredTime);
        } else if (Objects.equals(ConsumeStrategy.SHARED, consumeStrategy)) {
            return new SharedConsumeMessageExecutor(subject, consumerGroup, partitionName, partitionExecutor, messageHandler, messageHandleExecutor, consumptionExpiredTime);
        } else {
            throw new IllegalArgumentException(String.format("不支持的 consumeStrategy %s", consumeStrategy));
        }
    }
}
