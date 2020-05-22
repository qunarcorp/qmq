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

package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.BaseMessageHandler;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class PushConsumerImpl extends BaseMessageHandler implements PushConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushConsumer.class);

    private final ConsumeParam consumeParam;
    private final LinkedBlockingQueue<PulledMessage> messageBuffer = new LinkedBlockingQueue<>();

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;

    PushConsumerImpl(String subject, String group, RegistParam param) {
        super(param.getExecutor(), param.getMessageListener());
        this.consumeParam = new ConsumeParam(subject, group, param);

        String[] values = {subject, group};
        Metrics.gauge("qmq_pull_buffer_size", SUBJECT_GROUP_ARRAY, values, new Supplier<Double>() {
            @Override
            public Double get() {
                return (double) messageBuffer.size();
            }
        });
        this.createToHandleTimer = Metrics.timer("qmq_pull_createToHandle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleTimer = Metrics.timer("qmq_pull_handle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleFailCounter = Metrics.counter("qmq_pull_handleFail_count", SUBJECT_GROUP_ARRAY, values);
    }

    @Override
    public String subject() {
        return consumeParam.getSubject();
    }

    @Override
    public String group() {
        return consumeParam.getGroup();
    }

    @Override
    public ConsumeParam consumeParam() {
        return consumeParam;
    }

    @Override
    public boolean cleanLocalBuffer() {
        while (!messageBuffer.isEmpty()) {
            if (!push(messageBuffer.peek())) {
                return false;
            } else {
                messageBuffer.poll();
            }
        }
        return true;
    }

    @Override
    public void push(List<PulledMessage> messages) {
        for (int i = 0; i < messages.size(); i++) {
            final PulledMessage message = messages.get(i);
            if (!push(message)) {
                messageBuffer.addAll(messages.subList(i, messages.size()));
                break;
            }
        }
    }

    private boolean push(PulledMessage message) {
        HandleTaskImpl task = new HandleTaskImpl(message, this);
        try {
            executor.execute(task);
            LOGGER.info("进入执行队列 {}:{}", message.getSubject(), message.getMessageId());
            return true;
        } catch (RejectedExecutionException e) {
            LOGGER.error("消息进入执行队列失败，请调整消息处理线程池大小, {}:{}", message.getSubject(), message.getMessageId());
            return false;
        }
    }

    @Override
    protected void ack(BaseMessage message, long elapsed, Throwable exception, Map<String, String> attachment) {
        PulledMessage pulledMessage = (PulledMessage) message;
        if (pulledMessage.hasNotAcked()) {
            AckHelper.ackWithTrace(pulledMessage, exception);
        }
    }

    private final class HandleTaskImpl extends HandleTask {
        private final PulledMessage message;

        HandleTaskImpl(PulledMessage message, BaseMessageHandler handler) {
            super(message, handler);
            this.message = message;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            createToHandleTimer.update(start - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
            try {
                super.run();
            } finally {
                handleTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                if (super.handleFail) {
                    handleFailCounter.inc();
                }
            }
        }
    }

    @Override
    public void call(PulledMessage message, Throwable throwable) {
        applyPostOnMessage(message, throwable, new HashMap<>(message.filterContext()));
        AckHelper.ackWithTrace(message, throwable);
    }
}
