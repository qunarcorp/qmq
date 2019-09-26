package qunar.tc.qmq.common;

import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.sender.MessageGroupResolver;
import qunar.tc.qmq.producer.sender.SendMessageExecutorManager;

import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class BestTriedOrderStrategy extends AbstractOrderStrategy {

    private MessageGroupResolver messageGroupResolver;

    public BestTriedOrderStrategy(MessageGroupResolver messageGroupResolver) {
        this.messageGroupResolver = messageGroupResolver;
    }

    @Override
    void doOnSendError(ProduceMessage message, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, Exception e) {
        if (message.getTries() >= message.getMaxTries()) {
            currentExecutor.removeMessage(message);
            message.failed();
            return;
        }
        MessageGroup messageGroup = messageGroupResolver.resolveAvailableGroup((BaseMessage) message.getBase());
        MessageGroup oldMessageGroup = currentExecutor.getMessageGroup();
        if (!Objects.equals(messageGroup, oldMessageGroup)) {
            currentExecutor.removeMessage(message);
            SendMessageExecutor newExecutor = sendMessageExecutorManager.getExecutor(messageGroup);
            newExecutor.addMessage(message);
        }
        message.incTries();
    }

    @Override
    public void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor, Throwable t) {
        if (message.isNotAcked()) {
            message.ackWithTrace(t);
        }
    }

    @Override
    public void onMessageNotAcked(PulledMessage message, ConsumeMessageExecutor executor) {
        // 直接跳过, 处理下一条消息
    }

    @Override
    public boolean isDeadRetry(int nextRetryCount, BaseMessage message) {
        return nextRetryCount > message.getMaxRetryNum();
    }

    @Override
    public String name() {
        return BEST_TRIED;
    }
}
