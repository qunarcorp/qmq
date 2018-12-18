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

package qunar.tc.qmq.processor.filters;

import qunar.tc.qmq.common.Disposable;

import java.util.ArrayList;
import java.util.List;

/**
 * User: zhaohuiyu Date: 4/2/13 Time: 12:14 PM
 */
public class ReceiveFilterChain implements Disposable {
    private final List<ReceiveFilter> filters = new ArrayList<>();

    public ReceiveFilterChain() {
        addFilter(new ValidateFilter());
    }

    public Invoker buildFilterChain(Invoker invoker) {
        Invoker last = invoker;
        if (filters.size() > 0) {
            for (int i = filters.size() - 1; i >= 0; i--) {
                final ReceiveFilter filter = filters.get(i);
                final Invoker next = last;
                last = message -> filter.invoke(next, message);
            }
        }
        return last;
    }

    private void addFilter(ReceiveFilter filter) {
        filters.add(filter);
    }

    @Override
    public void destroy() {
        if (filters != null && !filters.isEmpty()) {
            for (ReceiveFilter filter : filters) {
                if (filter instanceof Disposable) {
                    ((Disposable) filter).destroy();
                }
            }
        }
    }
}

