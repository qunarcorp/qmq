package qunar.tc.qmq.consumer;

import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class MessageExecutionTask implements Runnable {

    private final AtomicInteger localRetries = new AtomicInteger(0);  // 本地重试次数
    private final AtomicBoolean handleFail = new AtomicBoolean(false);

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;

    private final PulledMessage message;
    private final MessageHandler handler;
    private final Executor executor;


    public MessageExecutionTask(PulledMessage message, Executor executor, MessageHandler handler,
                                QmqTimer createToHandleTimer, QmqTimer handleTimer, QmqCounter handleFailCounter) {
        this.message = message;
        this.executor = executor;
        this.handler = handler;
        this.createToHandleTimer = createToHandleTimer;
        this.handleTimer = handleTimer;
        this.handleFailCounter = handleFailCounter;
    }

    @Override
    public void run() {
        process();
    }

    public boolean process() {
        long start = System.currentTimeMillis();
        createToHandleTimer.update(start - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
        try {
            message.setProcessThread(Thread.currentThread());
            final Map<String, Object> filterContext = new HashMap<>();
            message.localRetries(localRetries.get());
            message.filterContext(filterContext);

            Throwable exception = null;
            boolean reQueued = false;
            try {
                if (!handler.preHandle(message, filterContext)) {
                    return true;
                }
                handler.handle(message);
                return true;
            } catch (NeedRetryException e) {
                exception = e;
                try {
                    reQueued = localRetry(e);
                } catch (Throwable ex) {
                    exception = ex;
                }
                return reQueued;
            } catch (Throwable e) {
                exception = e;
                return false;
            } finally {
                postHandle(reQueued, start, exception, filterContext);
            }
        } finally {
            handleTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
            if (handleFail.get()) {
                handleFailCounter.inc();
            }
        }
    }

    private boolean localRetry(NeedRetryException e) {
        boolean reQueued = false;
        if (isRetryImmediately(e)) {
            TraceUtil.recordEvent("local_retry");
            try {
                localRetries.incrementAndGet();
                if (executor != null) {
                    executor.execute(this);
                } else {
                    this.process();
                }
                reQueued = true;
            } catch (RejectedExecutionException re) {
                message.localRetries(localRetries.get());
                try {
                    handler.handle(message);
                } catch (NeedRetryException ne) {
                    localRetry(ne);
                }
            }
        }
        return reQueued;
    }

    private boolean isRetryImmediately(NeedRetryException e) {
        long next = e.getNext();
        return next - System.currentTimeMillis() <= 50;
    }

    private void postHandle(boolean reQueued, long start, Throwable exception, Map<String, Object> filterContext) {
        handleFail.set(exception != null);
        if (message.isAutoAck() || exception != null) {
            handler.postHandle(message, exception, filterContext);

            if (reQueued) return;
            handler.ack(message, System.currentTimeMillis() - start, exception, null);
        }
    }
}
