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
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

public class BufferedMessageExecutor implements MessageExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedMessageExecutor.class);

    private final LinkedBlockingQueue<PulledMessage> messageBuffer = new LinkedBlockingQueue<>();

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;
    private final Executor executor;
    private final MessageHandler messageHandler;

    public BufferedMessageExecutor(String subject, String group, RegistParam param) {
        this.executor = param.getExecutor();
        this.messageHandler = new BaseMessageHandler(param.getMessageListener());

        String[] values = {subject, group};
        Metrics.gauge("qmq_pull_buffer_size", SUBJECT_GROUP_ARRAY, values, () -> (double) messageBuffer.size());
        this.createToHandleTimer = Metrics.timer("qmq_pull_createToHandle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleTimer = Metrics.timer("qmq_pull_handle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleFailCounter = Metrics.counter("qmq_pull_handleFail_count", SUBJECT_GROUP_ARRAY, values);
    }

    @Override
    public boolean cleanUp() {
        while (!messageBuffer.isEmpty()) {
            if (!execute(messageBuffer.peek())) {
                return false;
            } else {
                messageBuffer.poll();
            }
        }
        return true;
    }

    @Override
    public void execute(List<PulledMessage> messages) {
        for (int i = 0; i < messages.size(); i++) {
            final PulledMessage message = messages.get(i);
            if (!execute(message)) {
                messageBuffer.addAll(messages.subList(i, messages.size()));
                break;
            }
        }
    }

    @Override
    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    private boolean execute(PulledMessage message) {
        ExecutionTask task = new ExecutionTask(message, messageHandler);
        try {
            executor.execute(task);
            LOGGER.info("进入执行队列 {}:{}", message.getSubject(), message.getMessageId());
            return true;
        } catch (RejectedExecutionException e) {
            LOGGER.error("消息进入执行队列失败，请调整消息处理线程池大小, {}:{}", message.getSubject(), message.getMessageId());
            return false;
        }
    }

    private class ExecutionTask implements Runnable {

        private final PulledMessage message;
        private final MessageHandler handler;

        private volatile int localRetries = 0;  // 本地重试次数
        private volatile boolean handleFail = false;

        ExecutionTask(PulledMessage message, MessageHandler handler) {
            this.message = message;
            this.handler = handler;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            createToHandleTimer.update(start - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
            try {
                message.setProcessThread(Thread.currentThread());
                final Map<String, Object> filterContext = new HashMap<>();
                message.localRetries(localRetries);
                message.filterContext(filterContext);

                Throwable exception = null;
                boolean reQueued = false;
                try {
                    if (!handler.preHandle(message, filterContext)) return;
                    handler.handle(message);
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
                    postHandle(reQueued, start, exception, filterContext);
                }
            } finally {
                handleTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                if (handleFail) {
                    handleFailCounter.inc();
                }
            }
        }

        private boolean localRetry(NeedRetryException e) {
            boolean reQueued = false;
            if (isRetryImmediately(e)) {
                TraceUtil.recordEvent("local_retry");
                try {
                    ++localRetries;
                    BufferedMessageExecutor.this.executor.execute(this);
                    reQueued = true;
                } catch (RejectedExecutionException re) {
                    message.localRetries(localRetries);
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
            handleFail = exception != null;
            if (message.isAutoAck() || exception != null) {
                handler.postHandle(message, exception, filterContext);

                if (reQueued) return;
                handler.ack(message, System.currentTimeMillis() - start, exception, null);
            }
        }

    }
}
