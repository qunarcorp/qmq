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

package qunar.tc.qmq.broker;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerClusterInfo {
    private final List<BrokerGroupInfo> groupList;
    private final Map<String, BrokerGroupInfo> groupMap;

    public BrokerClusterInfo() {
        this.groupList = Collections.emptyList();
        this.groupMap = Collections.emptyMap();
    }

    public BrokerClusterInfo(List<BrokerGroupInfo> groupList) {
        this.groupList = groupList;
        this.groupMap = Maps.newHashMapWithExpectedSize(groupList.size());
        for (BrokerGroupInfo group : groupList) {
            groupMap.put(group.getGroupName(), group);
        }
    }

    public List<BrokerGroupInfo> getGroups() {
        return groupList;
    }

    public BrokerGroupInfo getGroupByName(String groupName) {
        return groupMap.get(groupName);
    }

    @Override
    public String toString() {
        return "BrokerClusterInfo{groups=" + groupList + "}";
    }
}
