package qunar.tc.qmq.common;

import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.batch.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.QueueSender;
import qunar.tc.qmq.producer.sender.MessageGroupResolver;

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
    void doOnError(ProduceMessage message, QueueSender sender, SendMessageExecutor currentExecutor, Exception e) {
        if (message.getTries() > message.getMaxTries()) {
            currentExecutor.removeMessage(message);
            return;
        }
        MessageGroup messageGroup = messageGroupResolver.resolveAvailableGroup(message.getBase());
        MessageGroup oldMessageGroup = currentExecutor.getMessageGroup();
        if (!Objects.equals(messageGroup, oldMessageGroup)) {
            currentExecutor.removeMessage(message);
            sender.offer(message, messageGroup);
        }
    }

    @Override
    public void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor) {

    }

    @Override
    public void onMessageNotAcked(PulledMessage message, ConsumeMessageExecutor executor) {

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
