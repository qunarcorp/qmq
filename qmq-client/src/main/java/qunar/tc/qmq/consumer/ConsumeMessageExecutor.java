package qunar.tc.qmq.consumer;

import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public interface ConsumeMessageExecutor {

    boolean consume(List<PulledMessage> messages);

    /**
     * 将消息重新放到队列第一个
     *
     * @param pulledMessage 消息
     * @return 重试结果
     */
    boolean requeueFirst(PulledMessage pulledMessage);

    MessageHandler getMessageHandler();

    boolean cleanUp();

    void destroy();
}
