package qunar.tc.qmq.common;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.SendMessageExecutor;
import qunar.tc.qmq.producer.QueueSender;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public abstract class AbstractOrderStrategy implements OrderStrategy {

    @Override
    public void onSendError(ProduceMessage message, QueueSender sender, SendMessageExecutor currentExecutor, Exception e) {
        message.reset();
        doOnError(message, sender, currentExecutor, e);
    }

    abstract void doOnError(ProduceMessage message, QueueSender sender, SendMessageExecutor currentExecutor, Exception e);

    @Override
    public void onSendFailed(ProduceMessage pm, QueueSender sender, SendMessageExecutor currentExecutor, Exception e) {
        currentExecutor.removeMessage(pm);
        pm.failed();
    }

    @Override
    public void onSendBlocked(ProduceMessage pm, QueueSender sender, SendMessageExecutor currentExecutor, MessageException ex) {
        currentExecutor.removeMessage(pm);
        pm.block();
    }

    @Override
    public void onSendSuccessful(ProduceMessage pm, QueueSender sender, SendMessageExecutor currentExecutor, Exception e) {
        currentExecutor.removeMessage(pm);
        pm.finish();
    }

    @Override
    public void onSendFinished(List<ProduceMessage> sourceMessages, QueueSender sender, SendMessageExecutor currentExecutor) {
        currentExecutor.reset();
    }
}
