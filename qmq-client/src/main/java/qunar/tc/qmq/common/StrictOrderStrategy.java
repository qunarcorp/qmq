package qunar.tc.qmq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.sender.MessageGroupResolver;
import qunar.tc.qmq.producer.sender.SendMessageExecutor;
import qunar.tc.qmq.producer.sender.SendMessageExecutorManager;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class StrictOrderStrategy extends AbstractOrderStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrictOrderStrategy.class);

    private final MessageGroupResolver messageGroupResolver;

    public StrictOrderStrategy(MessageGroupResolver messageGroupResolver) {
        this.messageGroupResolver = messageGroupResolver;
    }

    @Override
    public void doOnSendError(ProduceMessage message, SendMessageExecutor currentExecutor,
            SendMessageExecutorManager sendMessageExecutorManager, Exception e) {
        MessageGroup messageGroup = currentExecutor.getMessageGroup();
        message.incTries();
        LOGGER.error("消息发送失败, 将进行重试, subject {} partition {} brokerGroup {}",
                messageGroup.getSubject(), messageGroup.getPartitionName(), messageGroup.getBrokerGroup());
    }

    @Override
    public MessageGroup resolveMessageGroup(BaseMessage message) {
        return messageGroupResolver.resolveGroup(message);
    }

    @Override
    public void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor, Throwable t) {
        executor.requeueFirst(message);
        String subject = executor.getSubject();
        String consumerGroup = executor.getConsumerGroup();
        String messageId = message.getMessageId();
        LOGGER.error("消息消费失败, 重新入队重试, subject {} consumerGroup {} messageId {}", subject,
                consumerGroup, messageId);
    }

    @Override
    public void onMessageNotAcked(PulledMessage message, ConsumeMessageExecutor executor) {
        executor.requeueFirst(message);
        String subject = executor.getSubject();
        String consumerGroup = executor.getConsumerGroup();
        String messageId = message.getMessageId();
        LOGGER.error("消息未 ACK, 将进行重复消费, 请检查程序是否存在 BUG, subject {} consumerGroup {} messageId {}", subject,
                consumerGroup, messageId);
    }

    @Override
    public boolean isDeadRetry(int nextRetryCount, BaseMessage message) {
        return true;
    }

    @Override
    public String name() {
        return STRICT;
    }
}
