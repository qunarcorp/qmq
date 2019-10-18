package qunar.tc.qmq.common;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutor;
import qunar.tc.qmq.producer.sender.SendMessageExecutorManager;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class BestTriedOrderStrategy extends AbstractOrderStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(BestTriedOrderStrategy.class);

    private MessageGroupResolver messageGroupResolver;

    public BestTriedOrderStrategy(MessageGroupResolver messageGroupResolver) {
        this.messageGroupResolver = messageGroupResolver;
    }

    @Override
    void doOnSendError(ProduceMessage message, SendMessageExecutor currentExecutor,
            SendMessageExecutorManager sendMessageExecutorManager, Exception e) {

        Message baseMessage = message.getBase();
        MessageGroup oldMessageGroup = currentExecutor.getMessageGroup();
        if (message.getTries() >= message.getMaxTries()) {
            String subject = oldMessageGroup.getSubject();
            String partitionName = oldMessageGroup.getPartitionName();
            String brokerGroup = oldMessageGroup.getBrokerGroup();
            LOGGER.error("消息发送失败, 达到最大重试次数 {}, subject {} partition {} brokerGroup {}", message.getMaxTries(), subject,
                    partitionName, brokerGroup);
            currentExecutor.removeMessage(message);
            message.failed();
            return;
        }

        MessageGroup newMessageGroup = messageGroupResolver.resolveAvailableGroup((BaseMessage) baseMessage);
        if (!Objects.equals(newMessageGroup, oldMessageGroup)) {
            currentExecutor.removeMessage(message);
            SendMessageExecutor newExecutor = sendMessageExecutorManager.getExecutor(newMessageGroup);
            newExecutor.addMessage(message);
        }
        message.incTries();

        LOGGER.error("消息发送失败, 将进行重试, subject {} partition {} brokerGroup {}",
                newMessageGroup.getSubject(), newMessageGroup.getPartitionName(), newMessageGroup.getBrokerGroup(), e);
    }

    @Override
    public MessageGroup resolveMessageGroup(BaseMessage message) {
        return messageGroupResolver.resolveAvailableGroup(message);
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
