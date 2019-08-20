package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedMessagePreHandler implements MessagePreHandler {

    @Override
    public void handle(List<ProduceMessage> messages) {
        messages.forEach(message -> {
            BaseMessage baseMessage = (BaseMessage) message.getBase();
            baseMessage.removeProperty(keys.qmq_queueSenderType);
            baseMessage.removeProperty(keys.qmq_queueLoadBalanceType);
        });
    }
}
