package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class MessageConsumptionTask {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumptionTask.class);

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;

    private final ConsumeMessageExecutor messageExecutor;
    private final PulledMessage message;
    private final MessageHandler handler;

    public MessageConsumptionTask(
            PulledMessage message,
            MessageHandler handler,
            ConsumeMessageExecutor messageExecutor,
            QmqTimer createToHandleTimer,
            QmqTimer handleTimer,
            QmqCounter handleFailCounter) {
        this.message = message;
        this.handler = handler;
        this.messageExecutor = messageExecutor;
        this.createToHandleTimer = createToHandleTimer;
        this.handleTimer = handleTimer;
        this.handleFailCounter = handleFailCounter;
    }

    public void run(Executor executor) {
        executor.execute(this::run);
    }

    public void run() {
        String subject = message.getSubject();
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(subject);
        try {
            processMessage();
            if (!message.isAutoAck() && message.isNotAcked()) {
                orderStrategy.onMessageNotAcked(message, messageExecutor);
            } else {
                orderStrategy.onConsumeSuccess(message, messageExecutor);
            }
        } catch (NeedRetryException e) {
            // 如果非 RetryImmediately 会抛出 NeedRetry 异常
            if (!localRetry(message, e)) {
                throw e;
            }
        } catch (Throwable t) {
            handleFailCounter.inc();
            logger.error("消息处理失败 {} {}", message.getSubject(), message.getMessageId(), t);
            orderStrategy.onConsumeFailed(message, messageExecutor, t);
        }
    }

    private void processMessage() {
        Throwable exception = null;
        long start = System.currentTimeMillis();
        final Map<String, Object> filterContext = new HashMap<>();
        try {
            createToHandleTimer.update(start - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
            message.setProcessThread(Thread.currentThread());
            message.filterContext(filterContext);
            if (!handler.preHandle(message, filterContext)) {
                return;
            }
            handler.handle(message);
        } catch (Throwable e) {
            exception = e;
            throw e;
        } finally {
            handleTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
            handler.postHandle(message, exception, filterContext);
        }
    }

    private boolean localRetry(PulledMessage message, NeedRetryException e) {
        if (isRetryImmediately(e)) {
            TraceUtil.recordEvent("local_retry");
            message.localRetries(message.localRetries() + 1);
            run();
            return true;
        } else {
            return false;
        }
    }

    private boolean isRetryImmediately(NeedRetryException e) {
        return (e.getNext() - System.currentTimeMillis()) <= 50;
    }

}
