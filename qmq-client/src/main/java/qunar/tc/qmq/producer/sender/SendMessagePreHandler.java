package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.broker.BrokerService;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public interface SendMessagePreHandler {

    void handle(List<ProduceMessage> messages);

    void init(BrokerService brokerService);
}
