package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Maps;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.MessageGroupResolver;
import qunar.tc.qmq.producer.ConfigCenter;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
public class DefaultSendMessageExecutorManager implements SendMessageExecutorManager {

    // subject-physicalPartition => executor
    private Map<MessageGroup, DefaultSendMessageExecutor> executorMap = Maps.newConcurrentMap();

    private int sendBatch;
    private MessageSender messageSender;
    private ExecutorService executor;
    private MessageGroupResolver messageGroupResolver;

    public DefaultSendMessageExecutorManager(
            MessageSender messageSender,
            ExecutorService executor,
            MessageGroupResolver messageGroupResolver) {
        ConfigCenter configCenter = ConfigCenter.getInstance();
        this.sendBatch = configCenter.getSendBatch();
        this.executor = executor;
        this.messageGroupResolver = messageGroupResolver;
        this.messageSender = messageSender;
    }

    @Override
    public StatefulSendMessageExecutor getExecutor(ProduceMessage produceMessage) {
        BaseMessage baseMessage = (BaseMessage) produceMessage.getBase();
        MessageGroup messageGroup = messageGroupResolver.resolveGroup(baseMessage);
        produceMessage.setMessageGroup(messageGroup);
        return getExecutor(messageGroup);
    }

    @Override
    public StatefulSendMessageExecutor getExecutor(MessageGroup messageGroup) {
        return executorMap.computeIfAbsent(messageGroup, group -> new DefaultSendMessageExecutor(messageGroup, this, sendBatch, messageSender, executor));
    }
}
