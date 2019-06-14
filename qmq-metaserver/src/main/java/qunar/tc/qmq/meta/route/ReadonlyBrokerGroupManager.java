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

package qunar.tc.qmq.meta.route;

import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author yunfeng.yang
 * @since 2018/3/1
 */
public class ReadonlyBrokerGroupManager {
    private final CachedMetaInfoManager cachedMetaInfoManager;

    public ReadonlyBrokerGroupManager(final CachedMetaInfoManager cachedMetaInfoManager) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
    }

    public List<BrokerGroup> disableReadonlyBrokerGroup(String realSubject, int clientTypeCode, List<BrokerGroup> brokerGroups) {
        if (clientTypeCode != ClientType.PRODUCER.getCode()
                && clientTypeCode != ClientType.DELAY_PRODUCER.getCode()) {
            return brokerGroups;
        }

        List<BrokerGroup> result = new ArrayList<>();
        for (BrokerGroup brokerGroup : brokerGroups) {
            if (isReadonlyForSubject(realSubject, brokerGroup.getGroupName())) {
                continue;
            }

            result.add(brokerGroup);
        }

        return result;
    }

    private boolean isReadonlyForSubject(final String subject, final String brokerGroup) {
        final Set<String> subjects = cachedMetaInfoManager.getBrokerGroupReadonlySubjects(brokerGroup);
        if (subjects == null) {
            return false;
        }

        return subjects.contains(subject) || subjects.contains("*");
    }
}
