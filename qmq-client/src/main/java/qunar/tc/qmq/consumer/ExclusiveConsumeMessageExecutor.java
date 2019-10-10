package qunar.tc.qmq.consumer;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.Objects;
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
            String subject = message.getSubject();
            OrderStrategy strategy = OrderStrategyCache.getStrategy(subject);
            if (Objects.equals(strategy.name(), OrderStrategy.STRICT) && System.currentTimeMillis() > getConsumptionExpiredTime()) {
                // 没有权限, 停一会再看
                requeueFirst(message);
                Thread.sleep(10);
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
