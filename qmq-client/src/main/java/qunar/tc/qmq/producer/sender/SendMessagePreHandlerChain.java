package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Lists;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.broker.BrokerService;

import java.util.List;
import java.util.ServiceLoader;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class SendMessagePreHandlerChain implements SendMessagePreHandler {

    private final List<SendMessagePreHandler> handlers;

    public SendMessagePreHandlerChain() {
        this.handlers = Lists.newArrayList();
        for (SendMessagePreHandler handler : ServiceLoader.load(SendMessagePreHandler.class)) {
            handlers.add(handler);
        }
    }

    @Override
    public void handle(List<ProduceMessage> messages) {
        for (SendMessagePreHandler handler : handlers) {
            handler.handle(messages);
        }
    }

    @Override
    public void init(BrokerService brokerService) {
        for (SendMessagePreHandler handler : handlers) {
            handler.init(brokerService);
        }
    }
}
