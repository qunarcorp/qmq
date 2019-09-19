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
public class MessageConsumptionTask {

    private final AtomicInteger localRetries = new AtomicInteger(0);  // 本地重试次数
    private final AtomicBoolean handleFail = new AtomicBoolean(false);

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;

    private final PulledMessage message;
    private final MessageHandler handler;
    private volatile boolean isAcked = false;


    public MessageConsumptionTask(PulledMessage message, MessageHandler handler,
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

    public void run() {
        Throwable exception = null;
        boolean hasRetried = false;
        long start = System.currentTimeMillis();
        final Map<String, Object> filterContext = new HashMap<>();
        try {
            createToHandleTimer.update(start - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
            message.setProcessThread(Thread.currentThread());
            message.localRetries(localRetries.get());
            message.filterContext(filterContext);

            if (!handler.preHandle(message, filterContext)) {
                return;
            }
            handler.handle(message);
        } catch (NeedRetryException e) {
            exception = e;
            try {
                hasRetried = localRetry(e);
            } catch (Throwable ex) {
                exception = ex;
            }
        } catch (Throwable e) {
            exception = e;
        } finally {
            handleTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
            if (handleFail.get()) {
                handleFailCounter.inc();
            }
            postHandle(hasRetried, start, exception, filterContext);
        }
    }

    private boolean localRetry(NeedRetryException e) {
        if (isRetryImmediately(e)) {
            TraceUtil.recordEvent("local_retry");
            message.localRetries(localRetries.incrementAndGet());
            this.run();
            return true;
        }
        return false;
    }

    private boolean isRetryImmediately(NeedRetryException e) {
        long next = e.getNext();
        return next - System.currentTimeMillis() <= 50;
    }

    private void postHandle(boolean hasRetried, long start, Throwable exception, Map<String, Object> filterContext) {
        handleFail.set(exception != null);
        if (message.isAutoAck() || exception != null) {
            handler.postHandle(message, exception, filterContext);

            if (hasRetried) return;
            handler.ack(message, System.currentTimeMillis() - start, exception, null);
            isAcked = true;
        }
    }

    public boolean isAcked() {
        return isAcked;
    }
}
