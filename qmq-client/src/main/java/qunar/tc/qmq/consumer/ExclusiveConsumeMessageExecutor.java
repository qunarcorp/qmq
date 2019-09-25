package qunar.tc.qmq.consumer;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.concurrent.Executor;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class ExclusiveConsumeMessageExecutor extends AbstractConsumeMessageExecutor {

    public ExclusiveConsumeMessageExecutor(String subject, String consumerGroup, String partitionName, Executor partitionExecutor, MessageHandler messageHandler, long consumptionExpiredTime) {
        super(subject, consumerGroup, partitionName, partitionExecutor, messageHandler, consumptionExpiredTime);
    }

    @Override
    void processMessage(PulledMessage message) {
        try {
            if (System.currentTimeMillis() > getConsumptionExpiredTime()) {
                // 没有权限, 停一会再看
                Thread.sleep(10);
                requeueFirst(message);
                return;
            }

            // 独占消费使用单线程逐个任务处理
            MessageConsumptionTask task = new MessageConsumptionTask(message, getMessageHandler(), this, getCreateToHandleTimer(), getHandleTimer(), getHandleFailCounter());
            task.run();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public ConsumeStrategy getConsumeStrategy() {
        return ConsumeStrategy.EXCLUSIVE;
    }
}
