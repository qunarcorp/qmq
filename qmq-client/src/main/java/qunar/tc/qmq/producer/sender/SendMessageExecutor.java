package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.Stateful;

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
