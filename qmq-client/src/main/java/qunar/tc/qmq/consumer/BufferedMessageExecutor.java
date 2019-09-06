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
import qunar.tc.qmq.AbstractMessageExecutor;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.metrics.Metrics;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

public class BufferedMessageExecutor extends AbstractMessageExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedMessageExecutor.class);

    private final LinkedBlockingQueue<PulledMessage> messageBuffer = new LinkedBlockingQueue<>();

    private final Executor executor;
    private final MessageHandler messageHandler;

    public BufferedMessageExecutor(String subject, String group, Executor executor, MessageListener listener) {
        super(subject, group);
        Metrics.gauge("qmq_pull_buffer_size", SUBJECT_GROUP_ARRAY, new String[]{subject, group}, () -> (double) messageBuffer.size());
        this.executor = executor;
        this.messageHandler = new BaseMessageHandler(listener);
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
    public void destroy() {
        cleanUp();
    }

    @Override
    public boolean execute(List<PulledMessage> messages) {
        for (int i = 0; i < messages.size(); i++) {
            final PulledMessage message = messages.get(i);
            if (!execute(message)) {
                messageBuffer.addAll(messages.subList(i, messages.size()));
                break;
            }
        }
        return true;
    }

    @Override
    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    private boolean execute(PulledMessage message) {
        MessageExecutionTask task = new MessageExecutionTask(message, executor, messageHandler, getCreateToHandleTimer(), getHandleTimer(), getHandleFailCounter());
        try {
            executor.execute(task);
            LOGGER.info("进入执行队列 {}:{}", message.getSubject(), message.getMessageId());
            return true;
        } catch (RejectedExecutionException e) {
            LOGGER.error("消息进入执行队列失败，请调整消息处理线程池大小, {}:{}", message.getSubject(), message.getMessageId());
            return false;
        }
    }
}
