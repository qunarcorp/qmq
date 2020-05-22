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

package qunar.tc.qmq.consumer.annotation;

import qunar.tc.qmq.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * User: zhaohuiyu
 * Date: 9/16/14
 * Time: 3:16 PM
 */
class GeneratedListener implements MessageListener, FilterAttachable, IdempotentAttachable {
    private final Object bean;
    private final Method method;
    private final IdempotentChecker idempotentChecker;
    private final List<Filter> filters;

    GeneratedListener(Object bean, Method method, IdempotentChecker idempotentChecker, List<Filter> filters) {
        this.bean = bean;
        this.method = method;
        this.idempotentChecker = idempotentChecker;
        this.filters = filters;
    }

    @Override
    public void onMessage(Message msg) {
        try {
            this.method.invoke(bean, msg);
        } catch (InvocationTargetException e) {
            Throwable innerException = e.getTargetException();
            if (innerException instanceof NeedRetryException) {
                throw (NeedRetryException) innerException;
            }
            throw new RuntimeException("processor message error", innerException);
        } catch (Exception e) {
            throw new RuntimeException("processor message error", e);
        }
    }

    @Override
    public List<Filter> filters() {
        return filters;
    }

    @Override
    public IdempotentChecker getIdempotentChecker() {
        return idempotentChecker;
    }
}
