package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.QueueSender;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public interface OrderStrategy {

    void onSendError(ProduceMessage message, QueueSender sender, SendMessageExecutor currentExecutor, Exception e);

    void onSendFailed(ProduceMessage pm, QueueSender sender, SendMessageExecutor currentExecutor, Exception e);

    void onSendBlocked(ProduceMessage pm, QueueSender sender, SendMessageExecutor currentExecutor, MessageException ex);

    void onSendSuccessful(ProduceMessage pm, QueueSender sender, SendMessageExecutor currentExecutor, Exception e);

    void onSendFinished(List<ProduceMessage> sourceMessages, QueueSender sender, SendMessageExecutor currentExecutor);

    void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor);

    String name();
}
