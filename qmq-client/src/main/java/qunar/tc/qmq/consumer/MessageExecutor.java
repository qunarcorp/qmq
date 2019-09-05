package qunar.tc.qmq.consumer;

import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public interface MessageExecutor {

    boolean execute(List<PulledMessage> messages);

    MessageHandler getMessageHandler();

    boolean cleanUp();

    void destroy();
}
