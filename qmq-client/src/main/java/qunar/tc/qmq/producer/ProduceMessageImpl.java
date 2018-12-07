/*
 * Copyright 2018 Qunar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */
package qunar.tc.qmq.producer;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-5
 */
class ProduceMessageImpl implements ProduceMessage {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceMessageImpl.class);

    private static final int INIT = 0;
    private static final int QUEUED = 1;

    private static final int FINISH = 100;
    private static final int ERROR = -1;
    private static final int BLOCK = -2;

    private static final int DEFAULT_THREADS = 10;

    private static final int DEFAULT_QUEUE_SIZE = 1000;

    private static final Executor EXECUTOR;

    private static final QmqCounter sendCount = Metrics.counter("qmq_client_send_count");
    private static final QmqCounter sendOkCount = Metrics.counter("qmq_client_send_ok_count");
    private static final QmqCounter sendErrorCount = Metrics.counter("qmq_client_send_error_count");
    private static final QmqCounter sendFailCount = Metrics.counter("qmq_client_send_fail_count");
    private static final QmqCounter resendCount = Metrics.counter("qmq_client_resend_count");
    private static final QmqCounter enterQueueFail = Metrics.counter("qmq_client_enter_queue_fail");

    static {
        EXECUTOR = new ThreadPoolExecutor(1, DEFAULT_THREADS, 1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(DEFAULT_QUEUE_SIZE),
                new NamedThreadFactory("default-send-listener", true),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        LOGGER.error("MessageSendStateListener任务被拒绝,现在的大小为:threads-{}, queue size-{}.如果在该listener里执行了比较重的操作", DEFAULT_THREADS, DEFAULT_QUEUE_SIZE);
                    }
                });

    }

    /**
     * 最多尝试次数
     */
    private int sendTryCount;

    private final BaseMessage base;

    private final QueueSender sender;

    //tracer
    private final Tracer tracer;
    private Span traceSpan;
    private Scope traceScope;

    private MessageSendStateListener sendStateListener;

    private final AtomicInteger state = new AtomicInteger(INIT);
    private final AtomicInteger tries = new AtomicInteger(0);
    private boolean syncSend;

    public ProduceMessageImpl(BaseMessage base, QueueSender sender) {
        this.base = base;
        this.sender = sender;
        this.tracer = GlobalTracer.get();
    }

    @Override
    public void send() {
        sendCount.inc();
        attachTraceData();
        doSend();
    }

    private void doSend() {
        if (state.compareAndSet(INIT, QUEUED)) {
            tries.incrementAndGet();
            if (sendSync()) return;

            try (Scope scope = tracer.buildSpan("Qmq.QueueSender.Send").startActive(false)) {
                traceSpan = scope.span();

                if (sender.offer(this)) {
                    LOGGER.info("进入发送队列 {}:{}", getSubject(), getMessageId());
                } else {
                    enterQueueFail.inc();
                    LOGGER.info("内存发送队列已满! 此消息在用户进程阻塞,等待队列激活 {}:{}", getSubject(), getMessageId());
                    if (sender.offer(this, 50)) {
                        LOGGER.info("进入发送队列 {}:{}", getSubject(), getMessageId());
                    } else {
                        LOGGER.info("由于无法入队,发送失败！取消发送 {}:{}", getSubject(), getMessageId());
                        onFailed();
                    }
                }
            }

        } else {
            enterQueueFail.inc();
            throw new IllegalStateException("同一条消息不能被入队两次.");
        }
    }

    private boolean sendSync() {
        if (!syncSend) return false;
        sender.send(this);
        return true;
    }

    @Override
    public void finish() {
        state.set(FINISH);
        onSuccess();
        closeTrace();
    }

    private void onSuccess() {
        sendOkCount.inc();
        if (sendStateListener == null) return;
        EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                sendStateListener.onSuccess(base);
            }
        });
    }

    @Override
    public void error(Exception e) {
        state.set(ERROR);
        try {
            if (tries.get() < sendTryCount) {
                sendErrorCount.inc();
                LOGGER.info("发送失败, 重新发送. tryCount: {} {}:{}", tries.get(), getSubject(), getMessageId());
                resend();
            } else {
                failed();
            }
        } finally {
            closeTrace();
        }
    }

    @Override
    public void block() {
        try {
            state.set(BLOCK);
            LOGGER.info("消息被拒绝 {}:{}", getSubject(), getMessageId());
            if (syncSend) {
                throw new RuntimeException("消息被拒绝且没有store可恢复,请检查应用授权配置");
            }
        } finally {
            onFailed();
            closeTrace();
        }
    }

    @Override
    public void failed() {
        state.set(ERROR);
        try {
            sendErrorCount.inc();
            String message = "发送失败, 已尝试" + tries.get() + "次不再尝试重新发送.";
            LOGGER.info(message);

            if (syncSend) {
                throw new RuntimeException(message);
            }
        } finally {
            onFailed();
            closeTrace();
        }
    }

    private void onFailed() {
        TraceUtil.recordEvent("send_failed", tracer);
        sendFailCount.inc();
        if (sendStateListener == null) return;
        EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                sendStateListener.onFailed(base);
            }
        });
    }

    private void resend() {
        resendCount.inc();
        traceSpan = null;
        traceScope = null;
        state.set(INIT);
        TraceUtil.recordEvent("retry", tracer);
        doSend();
    }

    @Override
    public String getMessageId() {
        return base.getMessageId();
    }

    @Override
    public String getSubject() {
        return base.getSubject();
    }

    @Override
    public void startSendTrace() {
        if (traceSpan == null) return;
        traceScope = tracer.scopeManager().activate(traceSpan, false);
        attachTraceData();
    }

    private void attachTraceData() {
        TraceUtil.inject(base, tracer);
    }

    private void closeTrace() {
        if (traceScope == null) return;
        traceScope.close();
    }

    @Override
    public BaseMessage getBase() {
        return base;
    }

    public void setSendTryCount(int sendTryCount) {
        this.sendTryCount = sendTryCount;
    }

    public void setSendStateListener(MessageSendStateListener sendStateListener) {
        this.sendStateListener = sendStateListener;
    }

    public void setSyncSend(boolean syncSend) {
        this.syncSend = syncSend;
    }
}
