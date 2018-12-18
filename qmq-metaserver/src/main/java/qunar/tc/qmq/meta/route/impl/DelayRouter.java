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

import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * @author yiqun.fan create on 17-12-6.
 */
public class DelayRouter implements SubjectRouter {
    private static final int DEFAULT_MIN_NUM = 2;
    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final SubjectRouter internal;

    public DelayRouter(CachedMetaInfoManager cachedMetaInfoManager, SubjectRouter internal) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.internal = internal;
    }

    @Override
    public List<BrokerGroup> route(String realSubject, MetaInfoRequest request) {
        if (request.getClientTypeCode() == ClientType.DELAY_PRODUCER.getCode()) {
            return doRoute();
        } else {
            return internal.route(realSubject, request);
        }
    }

    private List<BrokerGroup> doRoute() {
        final List<BrokerGroup> delayGroups = cachedMetaInfoManager.getDelayNewGroups();
        final List<BrokerGroup> filterDelayGroups = filterNrwBrokers(delayGroups);
        return select(filterDelayGroups);
    }

    private List<BrokerGroup> filterNrwBrokers(final List<BrokerGroup> groups) {
        if (groups.isEmpty()) {
            return groups;
        }

        return groups.stream().filter(group -> group.getBrokerState() != BrokerState.NRW)
                .collect(Collectors.toList());
    }

    private List<BrokerGroup> select(final List<BrokerGroup> groups) {
        if (groups == null || groups.size() == 0) {
            return null;
        }
        if (groups.size() <= DEFAULT_MIN_NUM) {
            return groups;
        }

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final Set<BrokerGroup> resultSet = new HashSet<>(DEFAULT_MIN_NUM);
        while (resultSet.size() <= DEFAULT_MIN_NUM) {
            final int randomIndex = random.nextInt(groups.size());
            resultSet.add(groups.get(randomIndex));
        }

        return new ArrayList<>(resultSet);
    }
}
