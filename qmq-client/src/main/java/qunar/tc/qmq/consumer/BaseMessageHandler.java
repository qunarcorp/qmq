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

package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.*;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.handler.IdempotentCheckerFilter;
import qunar.tc.qmq.consumer.handler.QTraceFilter;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BaseMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessageHandler.class);

    protected final Executor executor;
    protected final MessageListener listener;
    private final List<Filter> filters;
    private final Filter qtraceFilter;

    public BaseMessageHandler(Executor executor, MessageListener listener) {
        this.executor = executor;
        this.listener = listener;
        this.filters = new ArrayList<>();
        buildFilterChain(listener);
        this.qtraceFilter = new QTraceFilter();
    }

    private void buildFilterChain(MessageListener listener) {
        if (listener instanceof FilterAttachable) {
            this.filters.addAll(FilterAttachable.class.cast(listener).filters());
        }
        if (listener instanceof IdempotentAttachable) {
            this.filters.add(new IdempotentCheckerFilter(IdempotentAttachable.class.cast(listener).getIdempotentChecker()));
        }
    }

    boolean triggerBeforeOnMessage(ConsumeMessage message, Map<String, Object> filterContext) {
        for (int i = 0; i < filters.size(); ++i) {
            message.processedFilterIndex(i);
            if (!filters.get(i).preOnMessage(message, filterContext)) {
                return false;
            }
        }
        return true;
    }

    protected void applyPostOnMessage(ConsumeMessage message, Throwable ex, Map<String, Object> filterContext) {
        int processedFilterIndex = message.processedFilterIndex();
        for (int i = processedFilterIndex; i >= 0; --i) {
            try {
                filters.get(i).postOnMessage(message, ex, filterContext);
            } catch (Throwable e) {
                LOGGER.error("post filter failed", e);
            }
        }
    }

    protected void ack(BaseMessage message, long elapsed, Throwable exception, Map<String, String> attachment) {

    }

    public static void printError(BaseMessage message, Throwable e) {
        if (e == null) return;
        if (e instanceof NeedRetryException) return;
        LOGGER.error("message process error. subject={}, msgId={}, times={}, maxRetryNum={}",
                message.getSubject(), message.getMessageId(), message.times(), message.getMaxRetryNum(), e);
    }

    public static class HandleTask implements Runnable {
        protected final ConsumeMessage message;
        private final BaseMessageHandler handler;
        private volatile int localRetries = 0;  // 本地重试次数
        protected volatile boolean handleFail = false;

        public HandleTask(ConsumeMessage message, BaseMessageHandler handler) {
            this.message = message;
            this.handler = handler;
        }

        @Override
        public void run() {
            message.setProcessThread(Thread.currentThread());
            final long start = System.currentTimeMillis();
            final Map<String, Object> filterContext = new HashMap<>();
            message.localRetries(localRetries);
            message.filterContext(filterContext);

            Throwable exception = null;
            boolean reQueued = false;
            try {
                handler.qtraceFilter.preOnMessage(message, filterContext);
                if (!handler.triggerBeforeOnMessage(message, filterContext)) return;
                handler.listener.onMessage(message);
            } catch (NeedRetryException e) {
                exception = e;
                try {
                    reQueued = localRetry(e);
                } catch (Throwable ex) {
                    exception = ex;
                }
            } catch (Throwable e) {
                exception = e;
            } finally {
                triggerAfterCompletion(reQueued, start, exception, filterContext);
                handler.qtraceFilter.postOnMessage(message, exception, filterContext);
            }
        }

        private boolean localRetry(NeedRetryException e) {
            boolean reQueued = false;
            if (isRetryImmediately(e)) {
                TraceUtil.recordEvent("local_retry");
                try {
                    ++localRetries;
                    handler.executor.execute(this);
                    reQueued = true;
                } catch (RejectedExecutionException re) {
                    message.localRetries(localRetries);
                    try {
                        handler.listener.onMessage(message);
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

        private void triggerAfterCompletion(boolean reQueued, long start, Throwable exception, Map<String, Object> filterContext) {
            handleFail = exception != null;
            if (message.isAutoAck() || exception != null) {
                handler.applyPostOnMessage(message, exception, filterContext);

                if (reQueued) return;
                handler.ack(message, System.currentTimeMillis() - start, exception, null);
            }
        }
    }
}
