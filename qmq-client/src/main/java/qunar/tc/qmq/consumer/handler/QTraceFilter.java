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

package qunar.tc.qmq.consumer.handler;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import qunar.tc.qmq.Filter;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.Map;

/**
 * Created by zhaohui.yu
 * 15/12/9
 */
public class QTraceFilter implements Filter {

    private static final String TRACE_OBJECT = "qtracer";

    private static final String TRACE_DESC = "Qmq.Consume.Process";

    private final Tracer tracer;

    public QTraceFilter() {
        tracer = GlobalTracer.get();
    }

    @Override
    public boolean preOnMessage(Message message, Map<String, Object> filterContext) {
        SpanContext context = TraceUtil.extract(message, tracer);
        Scope scope = tracer.buildSpan(TRACE_DESC)
                .withTag("subject", message.getSubject())
                .withTag("messageId", message.getMessageId())
                .withTag("localRetries", String.valueOf(message.localRetries()))
                .withTag("times", String.valueOf(message.times()))
                .asChildOf(context)
                .startActive(true);
        filterContext.put(TRACE_OBJECT, scope.span());
        return true;
    }

    @Override
    public void postOnMessage(Message message, Throwable e, Map<String, Object> filterContext) {
        Object o = filterContext.get(TRACE_OBJECT);
        if (!(o instanceof Span)) return;
        Scope scope = tracer.scopeManager().activate((Span) o, true);
        scope.close();
    }
}
