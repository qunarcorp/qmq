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

package qunar.tc.qmq.meta.route.impl;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.loadbalance.LoadBalance;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author yiqun.fan create on 17-12-6.
 */
public class DelayRouter implements SubjectRouter {
    private static final int DEFAULT_MIN_NUM = 2;
    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final SubjectRouter internal;
    private final LoadBalance loadBalance;

    public DelayRouter(CachedMetaInfoManager cachedMetaInfoManager, SubjectRouter internal, LoadBalance loadBalance) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.internal = internal;
        this.loadBalance=loadBalance;
    }

    @Override
    public List<BrokerGroup> route(String realSubject, MetaInfoRequest request) {
        if (request.getClientTypeCode() == ClientType.DELAY_PRODUCER.getCode()) {
            return doRoute(realSubject);
        } else {
            return internal.route(realSubject, request);
        }
    }

    private List<BrokerGroup> doRoute(String subject) {
        final List<BrokerGroup> delayGroups = cachedMetaInfoManager.getDelayNewGroups();
        final List<BrokerGroup> filterDelayGroups = filterNrwBrokers(delayGroups);
        if (filterDelayGroups == null || filterDelayGroups.size() == 0) {
            return Collections.EMPTY_LIST;
        }
        return loadBalance.select(subject, filterDelayGroups, DEFAULT_MIN_NUM);
    }

    private List<BrokerGroup> filterNrwBrokers(final List<BrokerGroup> groups) {
        if (groups.isEmpty()) {
            return groups;
        }

        return groups.stream().filter(group -> group.getBrokerState() != BrokerState.NRW)
                .collect(Collectors.toList());
    }

}
