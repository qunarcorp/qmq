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

import io.opentracing.propagation.TextMap;
import qunar.tc.qmq.Message;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class QmqMessageExtractAdapter implements TextMap {

    private final Map<String, String> map = new HashMap<>();

    public QmqMessageExtractAdapter(Message message) {
        Map<String, Object> attrs = message.getAttrs();
        for (Map.Entry<String, Object> entry : attrs.entrySet()) {
            if (TraceUtil.isTraceKey(entry.getKey())) {
                if (entry.getValue() == null) continue;
                map.put(TraceUtil.extractKey(entry.getKey()), entry.getValue().toString());
            }
        }
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException(
                "HeadersMapExtractAdapter should only be used with Tracer.extract()");
    }
}
