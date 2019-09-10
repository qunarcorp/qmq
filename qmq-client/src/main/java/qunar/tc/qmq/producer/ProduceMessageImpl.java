/*
 * Copyright 2018 Qunar, Inc.
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
 * limitations under the License.
 */
package qunar.tc.qmq.producer;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqMeter;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.concurrent.TimeUnit;
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

    private static final QmqCounter sendCount = Metrics.counter("qmq_client_send_count");
    private static final QmqCounter sendOkCount = Metrics.counter("qmq_client_send_ok_count");
    private static final QmqMeter sendOkQps = Metrics.meter("qmq_client_send_ok_qps");
    private static final QmqCounter sendErrorCount = Metrics.counter("qmq_client_send_error_count");
    private static final QmqCounter sendFailCount = Metrics.counter("qmq_client_send_fail_count");
    private static final QmqCounter resendCount = Metrics.counter("qmq_client_resend_count");
    private static final QmqCounter enterQueueFail = Metrics.counter("qmq_client_enter_queue_fail");

    private static final String persistenceTime = "qmq_client_persistence_time";
    private static final String[] persistenceTags = new String[]{"subject", "type"};

    private static final String persistenceThrowable = "qmq_client_persistence_throwable";


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
    private MessageStore store;
    private long sequence;

    //如果使用了分库分表等，用于临时记录分库分表的路由信息，确保在发送消息成功后能够根据路由信息找到插入的db
    private transient Object routeKey;
    private MessageGroup messageGroup;

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
                } else if (store != null) {
                    enterQueueFail.inc();
                    LOGGER.info("内存发送队列已满! 此消息将暂时丢弃,等待补偿服务处理 {}:{}", getSubject(), getMessageId());
                    failed();
                } else {
                    LOGGER.info("内存发送队列已满! 此消息在用户进程阻塞,等待队列激活 {}:{}", getSubject(), getMessageId());
                    if (sender.offer(this, 50)) {
                        LOGGER.info("进入发送队列 {}:{}", getSubject(), getMessageId());
                    } else {
                        enterQueueFail.inc();
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
        if (store != null || !syncSend) return false;
        sender.send(this);
        return true;
    }

    @Override
    public void finish() {
        state.set(FINISH);
        try {
            if (store == null) return;
            if (base.isStoreAtFailed()) return;
            long startTime = System.currentTimeMillis();
            store.finish(this);
            Metrics.timer(persistenceTime,
                    persistenceTags,
                    new String[]{getSubject(), "success"})
                    .update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            TraceUtil.recordEvent("Qmq.Store.Failed");
            Metrics.counter(persistenceThrowable, persistenceTags, new String[]{getSubject(), "success"}).inc();
        } finally {
            onSuccess();
            closeTrace();
        }
    }

    @Override
    public void reset() {
        LOGGER.info("消息状态重置 {}:{} tries:maxRetries {}:{}", getSubject(), getMessageId(), tries, getMaxTries());
        state.set(INIT);
    }

    private void onSuccess() {
        sendOkCount.inc();
        sendOkQps.mark();
        if (sendStateListener == null) return;
        sendStateListener.onSuccess(base);
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
            try {
                if (store == null) return;
                long startTime = System.currentTimeMillis();
                store.block(this);
                Metrics.timer(persistenceTime,
                        persistenceTags,
                        new String[]{getSubject(), "block"})
                        .update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                TraceUtil.recordEvent("Qmq.Store.Failed");
                Metrics.counter(persistenceThrowable, persistenceTags, new String[]{getSubject(), "block"}).inc();
            }
            LOGGER.info("消息被拒绝");
            if (store == null && syncSend) {
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
            try {
                if (store == null) return;
                if (base.isStoreAtFailed()) {
                    save();
                }
            } catch (Exception e) {
                TraceUtil.recordEvent("Qmq.Store.Failed");
            }

            if (store == null && syncSend) {
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
        sendStateListener.onFailed(base);
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

    @Override
    public void setStore(MessageStore messageStore) {
        this.store = messageStore;
    }

    @Override
    public void save() {
        long start = System.nanoTime();
        try {
            this.sequence = store.insertNew(this);
            Metrics.timer(persistenceTime,
                    persistenceTags,
                    new String[]{getSubject(), "save"}).update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            Metrics.counter(persistenceThrowable, persistenceTags, new String[]{this.getSubject(), "save"}).inc();
            throw e;
        }

    }

    @Override
    public long getSequence() {
        return this.sequence;
    }

    @Override
    public void setRouteKey(Object routeKey) {
        this.routeKey = routeKey;
    }

    @Override
    public Object getRouteKey() {
        return this.routeKey;
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

    @Override
    public int getTries() {
        return tries.get();
    }

    @Override
    public int getMaxTries() {
        return sendTryCount;
    }

    public void setMessageGroup(MessageGroup messageGroup) {
        this.messageGroup = messageGroup;
    }

    @Override
    public MessageGroup getMessageGroup() {
        return messageGroup;
    }
}
