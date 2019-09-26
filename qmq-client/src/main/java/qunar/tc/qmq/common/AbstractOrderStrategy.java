package qunar.tc.qmq.common;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutorManager;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public abstract class AbstractOrderStrategy implements OrderStrategy {

    @Override
    public void onSendError(ProduceMessage message, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, Exception e) {
        message.reset();
        doOnSendError(message, currentExecutor, sendMessageExecutorManager, e);
    }

    abstract void doOnSendError(ProduceMessage message, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, Exception e);

    @Override
    public void onSendBrokerError(ProduceMessage pm, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, Exception e) {
        currentExecutor.removeMessage(pm);
        pm.failed();
    }

    @Override
    public void onSendBlocked(ProduceMessage pm, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, MessageException ex) {
        currentExecutor.removeMessage(pm);
        pm.block();
    }

    @Override
    public void onSendSuccessful(ProduceMessage pm, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager) {
        currentExecutor.removeMessage(pm);
        pm.finish();
    }

    @Override
    public void onSendFinished(List<ProduceMessage> sourceMessages, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager) {
        currentExecutor.reset();
    }

    @Override
    public void onConsumeSuccess(PulledMessage message, ConsumeMessageExecutor executor) {
        if (message.isNotAcked()) {
            message.ackWithTrace(null);
        }
    }
}
