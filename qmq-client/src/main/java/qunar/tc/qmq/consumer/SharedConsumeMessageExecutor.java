package qunar.tc.qmq.consumer;

import java.util.concurrent.Executor;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.consumer.pull.PulledMessage;

/**
 * @author zhenwei.liu
 * @since 2019-09-19
 */
public class SharedConsumeMessageExecutor extends AbstractConsumeMessageExecutor {

    private final Executor messageHandleExecutor;

    public SharedConsumeMessageExecutor(
            String subject,
            String consumerGroup,
            String partitionName,
            Executor partitionExecutor,
            MessageHandler messageHandler,
            Executor messageHandleExecutor,
            long consumptionExpiredTime) {
        super(subject, consumerGroup, partitionName, partitionExecutor, messageHandler, consumptionExpiredTime);
        this.messageHandleExecutor = messageHandleExecutor;
    }

    @Override
    void processMessage(PulledMessage message) {
        MessageHandler messageHandler = getMessageHandler();
        MessageConsumptionTask task = new MessageConsumptionTask(message, messageHandler, this,
                getCreateToHandleTimer(), getHandleTimer(), getHandleFailCounter());
        task.run(messageHandleExecutor);
    }

    @Override
    public ConsumeStrategy getConsumeStrategy() {
        return ConsumeStrategy.SHARED;
    }
}
