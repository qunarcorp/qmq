package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.producer.OrderedMessageUtils;

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
            int physicalPartition = baseMessage.getIntProperty(keys.qmq_physicalPartition.name());
            baseMessage.setSubject(OrderedMessageUtils.getOrderedMessageSubject(baseMessage.getSubject(), physicalPartition));
            baseMessage.removeProperty(keys.qmq_queueSenderType);
            baseMessage.removeProperty(keys.qmq_loadBalanceType);
        });
    }
}
