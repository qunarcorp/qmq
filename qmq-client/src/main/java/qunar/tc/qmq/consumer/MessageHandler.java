package qunar.tc.qmq.consumer;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.pull.AckHook;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public interface MessageHandler extends AckHook {

    boolean preHandle(ConsumeMessage message, Map<String, Object> filterContext);

    void postHandle(ConsumeMessage message, Throwable ex, Map<String, Object> filterContext);

    void handle(Message msg);

    void ack(BaseMessage message, long elapsed, Throwable exception, Map<String, String> attachment);
}
