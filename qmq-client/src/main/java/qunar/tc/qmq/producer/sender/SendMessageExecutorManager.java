package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
public interface SendMessageExecutorManager {

    StatefulSendMessageExecutor getExecutor(ProduceMessage produceMessage);

    StatefulSendMessageExecutor getExecutor(MessageGroup messageGroup);

}
