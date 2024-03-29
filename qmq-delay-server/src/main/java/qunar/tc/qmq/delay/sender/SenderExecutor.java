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

package qunar.tc.qmq.delay.sender;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.PollBrokerLoadBalance;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.ScheduleIndex;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-16 21:00
 */
class SenderExecutor implements Disposable {
    private static final int DEFAULT_SEND_THREAD = 1;

    private final ConcurrentMap<String, SenderGroup> groupSenders = new ConcurrentHashMap<>();
    private final BrokerLoadBalance brokerLoadBalance;
    private final Sender sender;
    private final DelayLogFacade store;
    private final int sendThreads;
    private DynamicConfig sendConfig;

    SenderExecutor(final Sender sender, DelayLogFacade store, DynamicConfig sendConfig) {
        this.sender = sender;
        this.store = store;
        this.brokerLoadBalance = PollBrokerLoadBalance.getInstance();
        this.sendThreads = sendConfig.getInt("delay.send.threads", DEFAULT_SEND_THREAD);
        this.sendConfig = sendConfig;
    }

    void execute(final List<ScheduleIndex> indexList, final SenderGroup.ResultHandler handler, final BrokerService brokerService) {
        Map<SenderGroup, List<ScheduleIndex>> groups = groupByBroker(indexList, brokerService);
        for (Map.Entry<SenderGroup, List<ScheduleIndex>> entry : groups.entrySet()) {
            doExecute(entry.getKey(), entry.getValue(), handler);
        }
    }

    public void syncExecute(final List<ScheduleIndex> indexList, final SenderGroup.ResultHandler handler, final BrokerService brokerService) {
        Map<SenderGroup, List<ScheduleIndex>> groups = groupByBroker(indexList, brokerService);
        for (Map.Entry<SenderGroup, List<ScheduleIndex>> entry : groups.entrySet()) {
            doSyncExecute(entry.getKey(), entry.getValue(), handler);
        }
    }

    private void doSyncExecute(final SenderGroup group, final List<ScheduleIndex> list, final SenderGroup.ResultHandler handler) {
        group.sendSync(list, sender, handler);
    }

    private void doExecute(final SenderGroup group, final List<ScheduleIndex> list, final SenderGroup.ResultHandler handler) {
        group.send(list, sender, handler);
    }

    private Map<SenderGroup, List<ScheduleIndex>> groupByBroker(final List<ScheduleIndex> indexList, final BrokerService brokerService) {
        Map<SenderGroup, List<ScheduleIndex>> groups = Maps.newHashMap();
        Map<String, List<ScheduleIndex>> recordsGroupBySubject = groupBySubject(indexList);
        for (Map.Entry<String, List<ScheduleIndex>> entry : recordsGroupBySubject.entrySet()) {
            List<ScheduleIndex> setRecordsGroupBySubject = entry.getValue();
            BrokerGroupInfo groupInfo = loadGroup(entry.getKey(), brokerService);
            SenderGroup senderGroup = getGroup(groupInfo, sendThreads);

            List<ScheduleIndex> recordsInGroup = groups.get(senderGroup);
            if (null == recordsInGroup) {
                recordsInGroup = Lists.newArrayListWithCapacity(setRecordsGroupBySubject.size());
            }
            recordsInGroup.addAll(setRecordsGroupBySubject);
            groups.put(senderGroup, recordsInGroup);
        }

        return groups;
    }

    private SenderGroup getGroup(BrokerGroupInfo groupInfo, int sendThreads) {
        String groupName = groupInfo.getGroupName();
        SenderGroup senderGroup = groupSenders.get(groupName);
        if (null == senderGroup) {
            senderGroup = new SenderGroup(groupInfo, sendThreads, store, sendConfig);
            SenderGroup currentSenderGroup = groupSenders.putIfAbsent(groupName, senderGroup);
            senderGroup = null != currentSenderGroup ? currentSenderGroup : senderGroup;
        } else {
            senderGroup.reconfigureGroup(groupInfo);
        }

        return senderGroup;
    }

    private BrokerGroupInfo loadGroup(String subject, BrokerService brokerService) {
        BrokerClusterInfo cluster = brokerService.getClusterBySubject(ClientType.PRODUCER, subject);
        return brokerLoadBalance.loadBalance(cluster, null);
    }

    private Map<String, List<ScheduleIndex>> groupBySubject(List<ScheduleIndex> list) {
        Map<String, List<ScheduleIndex>> map = Maps.newHashMap();
        for (ScheduleIndex index : list) {
            List<ScheduleIndex> group = map.computeIfAbsent(index.getSubject(), k -> Lists.newArrayList());
            group.add(index);
        }

        return map;
    }

    @Override
    public void destroy() {
        groupSenders.values().parallelStream().forEach(SenderGroup::destroy);
        groupSenders.clear();
    }
}
