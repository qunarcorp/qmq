package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.concurrent.Executor;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class ExclusiveConsumeMessageExecutor extends AbstractConsumeMessageExecutor {

    private final Logger logger = LoggerFactory.getLogger(ExclusiveConsumeMessageExecutor.class);

    public ExclusiveConsumeMessageExecutor(String subject, String consumerGroup, String partitionName, Executor partitionExecutor, MessageHandler messageHandler, long consumptionExpiredTime) {
        super(subject, consumerGroup, partitionName, partitionExecutor, messageHandler, consumptionExpiredTime);
    }

    @Override
    void processMessage(PulledMessage message) {
        String subject = getSubject();
        MessageHandler messageHandler = getMessageHandler();
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(subject);
        try {
            if (System.currentTimeMillis() > getConsumptionExpiredTime()) {
                // 没有权限, 停一会再看
                Thread.sleep(10);
                requeueFirst(message);
                return;
            }

            // 独占消费使用单线程逐个任务处理
            MessageConsumptionTask task = new MessageConsumptionTask(message, messageHandler, getCreateToHandleTimer(), getHandleTimer(), getHandleFailCounter());
            task.run();
            if (!task.isAcked()) {
                // 严格有序消息如果没有 ACK 当做消费失败处理
                orderStrategy.onMessageNotAcked(message, this);
            }
        } catch (Throwable t) {
            orderStrategy.onConsumeFailed(message, this);
            logger.error("消息处理失败 ", t);
        }
    }

    @Override
    public ConsumeStrategy getConsumeStrategy() {
        return ConsumeStrategy.EXCLUSIVE;
    }
}
