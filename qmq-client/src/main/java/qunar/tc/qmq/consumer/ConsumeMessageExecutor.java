package qunar.tc.qmq.consumer;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public interface ConsumeMessageExecutor {

    String getSubject();

    String getConsumerGroup();

    String getPartitionName();

    ConsumeStrategy getConsumeStrategy();

    long getConsumptionExpiredTime();

    void setConsumptionExpiredTime(long timestamp);

    boolean consume(List<PulledMessage> messages);

    MessageHandler getMessageHandler();

    boolean requeueFirst(PulledMessage message);

    boolean isFull();

    void destroy();
}
