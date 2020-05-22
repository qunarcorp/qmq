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

import qunar.tc.qmq.Filter;
import qunar.tc.qmq.IdempotentChecker;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.Map;

/**
 * Created by zhaohui.yu
 * 15/12/8
 */
public class IdempotentCheckerFilter implements Filter {

    private final IdempotentChecker idempotentChecker;

    public IdempotentCheckerFilter(IdempotentChecker idempotentChecker) {
        this.idempotentChecker = idempotentChecker;
    }

    @Override
    public boolean preOnMessage(Message message, Map<String, Object> filterContext) {
        if (idempotentChecker == null) return true;
        TraceUtil.recordEvent("start idempotent");
        boolean processed = idempotentChecker.isProcessed(message);
        TraceUtil.recordEvent("end idempotent");
        if (processed) {
            TraceUtil.setTag("idempotent", "processed");
        }
        return !processed;
    }

    @Override
    public void postOnMessage(Message message, Throwable e, Map<String, Object> filterContext) {
        if (idempotentChecker == null) return;
        idempotentChecker.markProcessed(message, e);
    }
}
