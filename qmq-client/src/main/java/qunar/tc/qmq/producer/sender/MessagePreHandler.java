package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public interface MessagePreHandler {

    void handle(List<ProduceMessage> messages);
}
