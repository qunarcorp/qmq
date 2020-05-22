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

package qunar.tc.qmq.tracing;

import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.util.GlobalTracer;
import qunar.tc.qmq.Message;

public class TraceUtil {
    static final String TRACE_PREFIX = "qtrace_";

    static String extractKey(String messageKey) {
        return messageKey.substring(TRACE_PREFIX.length());
    }

    static boolean isTraceKey(String key) {
        if (key == null) return false;
        return key.startsWith(TRACE_PREFIX);
    }

    public static void inject(Message message, Tracer tracer) {
        Scope scope = tracer.scopeManager().active();
        if (scope == null) return;
        tracer.inject(scope.span().context(), Format.Builtin.TEXT_MAP, new QmqMessageInjectAdapter(message));
    }

    public static SpanContext extract(Message message, Tracer tracer) {
        return tracer.extract(Format.Builtin.TEXT_MAP, new QmqMessageExtractAdapter(message));
    }

    public static void setTag(String key, String value, Tracer tracer) {
        if (tracer == null) {
            tracer = GlobalTracer.get();
        }
        Scope scope = tracer.scopeManager().active();
        if (scope == null) return;
        scope.span().setTag(key, value);
    }

    public static void setTag(String key, String value) {
        setTag(key, value, null);
    }

    public static void recordEvent(String event, Tracer tracer) {
        if (tracer == null) {
            tracer = GlobalTracer.get();
        }
        Scope scope = tracer.scopeManager().active();
        if (scope == null) return;
        scope.span().log(event);
    }

    public static void recordEvent(String event) {
        recordEvent(event, null);
    }
}
