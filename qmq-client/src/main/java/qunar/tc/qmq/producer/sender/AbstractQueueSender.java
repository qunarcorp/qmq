package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Maps;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.producer.QueueSender;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public abstract class AbstractQueueSender implements QueueSender {

    protected RouterManager routerManager;

    protected Collection<MessageSenderGroup> groupBy(List<ProduceMessage> messages) {
        Map<Connection, MessageSenderGroup> map = Maps.newHashMap();
        for (int i = 0; i < messages.size(); ++i) {
            ProduceMessage produceMessage = messages.get(i);
            produceMessage.startSendTrace();
            Connection connection = routerManager.routeOf(produceMessage.getMessageGroup());
            MessageSenderGroup group = map.get(connection);
            if (group == null) {
                group = new MessageSenderGroup(connection);
                map.put(connection, group);
            }
            group.addMessage(produceMessage);
        }
        return map.values();
    }
}
