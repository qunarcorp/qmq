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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.*;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.handler.IdempotentCheckerFilter;
import qunar.tc.qmq.consumer.handler.QTraceFilter;
import qunar.tc.qmq.consumer.pull.AckHook;
import qunar.tc.qmq.consumer.pull.PulledMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BaseMessageHandler implements MessageHandler, AckHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessageHandler.class);

    protected final MessageListener listener;
    private final List<Filter> filters;
    private final Filter traceFilter;

    public BaseMessageHandler(MessageListener listener) {
        this.listener = listener;
        this.filters = buildFilterChain(listener);
        this.traceFilter = new QTraceFilter();
    }

    private List<Filter> buildFilterChain(MessageListener listener) {
        List<Filter> filters = Lists.newArrayList();
        if (listener instanceof FilterAttachable) {
            filters.addAll(((FilterAttachable) listener).filters());
        }
        if (listener instanceof IdempotentAttachable) {
            filters.add(new IdempotentCheckerFilter(((IdempotentAttachable) listener).getIdempotentChecker()));
        }
        return filters;
    }

    @Override
    public boolean preHandle(ConsumeMessage message, Map<String, Object> filterContext) {
        traceFilter.preOnMessage(message, filterContext);
        for (int i = 0; i < filters.size(); ++i) {
            message.processedFilterIndex(i);
            if (!filters.get(i).preOnMessage(message, filterContext)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void postHandle(ConsumeMessage message, Throwable ex, Map<String, Object> filterContext) {
        traceFilter.postOnMessage(message, ex, filterContext);
        int processedFilterIndex = message.processedFilterIndex();
        for (int i = processedFilterIndex; i >= 0; --i) {
            try {
                filters.get(i).postOnMessage(message, ex, filterContext);
            } catch (Throwable e) {
                LOGGER.error("post filter failed", e);
            }
        }
    }

    @Override
    public void handle(Message msg) {
        listener.onMessage(msg);
    }

    @Override
    public void ack(BaseMessage message, long elapsed, Throwable exception, Map<String, String> attachment) {
        PulledMessage pulledMessage = (PulledMessage) message;
        if (pulledMessage.isNotAcked()) {
            pulledMessage.ackWithTrace(exception);
        }
    }

    @Override
    public void call(PulledMessage message, Throwable throwable) {
        postHandle(message, throwable, new HashMap<>(message.filterContext()));
        message.ackWithTrace(throwable);
    }
}
