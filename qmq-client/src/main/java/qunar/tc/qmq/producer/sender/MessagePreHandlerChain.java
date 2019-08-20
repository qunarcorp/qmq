package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Lists;
import qunar.tc.qmq.ProduceMessage;

import java.util.List;
import java.util.ServiceLoader;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class MessagePreHandlerChain implements MessagePreHandler {

    private final List<MessagePreHandler> handlers;

    public MessagePreHandlerChain() {
        this.handlers = Lists.newArrayList();
        for (MessagePreHandler handler : ServiceLoader.load(MessagePreHandler.class)) {
            handlers.add(handler);
        }
    }

    @Override
    public void handle(List<ProduceMessage> messages) {
        for (MessagePreHandler handler : handlers) {
            handler.handle(messages);
        }
    }
}
