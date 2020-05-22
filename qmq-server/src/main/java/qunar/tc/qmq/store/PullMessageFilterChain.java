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

package qunar.tc.qmq.store;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.ServiceLoader;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.store.buffer.Buffer;

/**
 * @author keli.wang
 * @since 2019-01-02
 */
class PullMessageFilterChain {

	private List<PullMessageFilter> filters;

	PullMessageFilterChain(final DynamicConfig config) {
		this.filters = Lists.newArrayList(ServiceLoader.load(PullMessageFilter.class).iterator());
		this.filters.forEach(filter -> filter.init(config));
	}

	boolean needKeep(final PullRequest request, final Buffer message) {
		for (PullMessageFilter filter : filters) {
			if (filter.isActive(request, message) && !filter.match(request, message)) {
				return false;
			}
		}
		return true;
	}
}
