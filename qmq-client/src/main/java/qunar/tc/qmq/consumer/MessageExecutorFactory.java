package qunar.tc.qmq.consumer;

import qunar.tc.qmq.MessageListener;

import java.util.concurrent.Executor;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class MessageExecutorFactory {

    public static MessageExecutor createExecutor(String subject, String group, Executor executor, MessageListener listener, boolean isOrdered) {
        MessageExecutor messageExecutor;
        if (isOrdered) {
            messageExecutor = new OrderedMessageExecutor(subject, group, executor, listener);
            ((OrderedMessageExecutor) messageExecutor).start();
        } else {
            messageExecutor = new BufferedMessageExecutor(subject, group, executor, listener);
        }
        return messageExecutor;
    }
}
