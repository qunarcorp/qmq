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

package qunar.tc.qmq.delay.receiver.filter;

import com.google.common.collect.Lists;
import qunar.tc.qmq.delay.receiver.Invoker;

import java.util.List;

/**
 * 接收过滤器链
 *
 * @author kelly.li
 */
public class ReceiveFilterChain {
    private List<Filter> filters = Lists.newArrayList();

    public ReceiveFilterChain() {
        addFilter(new ValidateFilter());
        addFilter(new OverDelayFilter());
        addFilter(new PastDelayFilter());
    }

    public Invoker buildFilterChain(Invoker invoker) {
        Invoker last = invoker;
        if (0 < filters.size()) {
            for (int i = filters.size() - 1; i >= 0; i--) {
                final Filter filter = filters.get(i);
                final Invoker next = last;
                last = message -> filter.invoke(next, message);
            }
        }

        return last;
    }

    private void addFilter(Filter filter) {
        filters.add(filter);
    }
}
