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

package qunar.tc.qmq.broker.impl;

import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class PollBrokerLoadBalance implements BrokerLoadBalance {

    @Override
    public BrokerGroupInfo loadBalance(List<BrokerGroupInfo> groups, BrokerGroupInfo lastGroup) {
        if (lastGroup == null || lastGroup.getGroupIndex() < 0 || lastGroup.getGroupIndex() >= groups.size()) {
            BrokerGroupInfo group;
            for (int i = 0; i < groups.size(); i++) {
                if ((group = selectRandom(groups)).isAvailable()) {
                    return group;
                }
            }
            for (BrokerGroupInfo groupInfo : groups) {
                if (groupInfo.isAvailable()) {
                    return groupInfo;
                }
            }
        } else {
            int index = lastGroup.getGroupIndex();
            for (int count = groups.size(); count > 0; count--) {
                index = (index + 1) % groups.size();
                BrokerGroupInfo nextGroup = groups.get(index);
                if (nextGroup.isAvailable()) {
                    return nextGroup;
                }
            }
        }
        return lastGroup;
    }

    private BrokerGroupInfo selectRandom(List<BrokerGroupInfo> groups) {
        int random = ThreadLocalRandom.current().nextInt(groups.size());
        return groups.get(random);
    }
}
