package qunar.tc.qmq.batch;

import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public interface SendMessageExecutor<State> extends Stateful<State> {

    MessageGroup getMessageGroup();

    boolean addMessage(ProduceMessage message);

    boolean addMessage(ProduceMessage message, long timeoutMills);

    boolean removeMessage(ProduceMessage message);
}
