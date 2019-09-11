package qunar.tc.qmq.consumer;

import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.common.ClientLifecycleManagerFactory;

import java.util.concurrent.Executor;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class MessageExecutorFactory {

    public static ConsumeMessageExecutor createExecutor(String subject, String group, ConsumeMode consumeMode, Executor executor, MessageListener listener) {
        return new OrderedConsumeMessageExecutor(subject, group, consumeMode, executor, listener, ClientLifecycleManagerFactory.get());
    }
}
