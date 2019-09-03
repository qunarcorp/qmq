package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.base.BaseMessage;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class ConsumerUtils {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerUtils.class);

    public static void printError(BaseMessage message, Throwable e) {
        if (e == null) return;
        if (e instanceof NeedRetryException) return;
        logger.error("message process error. subject={}, msgId={}, times={}, maxRetryNum={}",
                message.getSubject(), message.getMessageId(), message.times(), message.getMaxRetryNum(), e);
    }
}
