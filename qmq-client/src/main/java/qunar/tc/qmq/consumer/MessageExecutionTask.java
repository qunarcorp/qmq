package qunar.tc.qmq.consumer;

import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class MessageExecutionTask {

    private final AtomicInteger localRetries = new AtomicInteger(0);  // 本地重试次数
    private final AtomicBoolean handleFail = new AtomicBoolean(false);

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;

    private final PulledMessage message;
    private final MessageHandler handler;


    public MessageExecutionTask(PulledMessage message, MessageHandler handler,
                                QmqTimer createToHandleTimer, QmqTimer handleTimer, QmqCounter handleFailCounter) {
        this.message = message;
        this.handler = handler;
        this.createToHandleTimer = createToHandleTimer;
        this.handleTimer = handleTimer;
        this.handleFailCounter = handleFailCounter;
    }

    public void run(Executor executor) {
        executor.execute(this::run);
    }

    public boolean run() {
        long start = System.currentTimeMillis();
        createToHandleTimer.update(start - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
        try {
            message.setProcessThread(Thread.currentThread());
            final Map<String, Object> filterContext = new HashMap<>();
            message.localRetries(localRetries.get());
            message.filterContext(filterContext);

            Throwable exception = null;
            boolean success = false;
            try {
                if (!handler.preHandle(message, filterContext)) {
                    return true;
                }
                handler.handle(message);
                success = true;
            } catch (NeedRetryException e) {
                exception = e;
                try {
                    success = localRetry(e);
                } catch (Throwable ex) {
                    exception = ex;
                }
            } catch (Throwable e) {
                exception = e;
            } finally {
                postHandle(success, start, exception, filterContext);
            }
            return success;
        } finally {
            handleTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
            if (handleFail.get()) {
                handleFailCounter.inc();
            }
        }
    }

    private boolean localRetry(NeedRetryException e) {
        if (isRetryImmediately(e)) {
            TraceUtil.recordEvent("local_retry");
            try {
                message.localRetries(localRetries.incrementAndGet());
                return this.run();
            } catch (NeedRetryException ne) {
                return localRetry(ne);
            }
        }
        return false;
    }

    private boolean isRetryImmediately(NeedRetryException e) {
        long next = e.getNext();
        return next - System.currentTimeMillis() <= 50;
    }

    private void postHandle(boolean success, long start, Throwable exception, Map<String, Object> filterContext) {
        handleFail.set(exception != null);
        if (message.isAutoAck() || exception != null) {
            handler.postHandle(message, exception, filterContext);

            if (!success) return;
            handler.ack(message, System.currentTimeMillis() - start, exception, null);
        }
    }
}
