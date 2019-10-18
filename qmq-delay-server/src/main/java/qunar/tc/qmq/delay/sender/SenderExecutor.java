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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.DefaultMessageGroupResolver;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.common.MessageGroupResolver;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.ScheduleIndex;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-16 21:00
 */
class SenderExecutor implements Disposable {

    private static final int DEFAULT_SEND_THREAD = 1;

    private final ConcurrentMap<String, GroupSender> groupSenders = new ConcurrentHashMap<>();
    private final Sender sender;
    private final DelayLogFacade store;
    private final int sendThreads;
    private final BrokerService brokerService;
    private final MessageGroupResolver messageGroupResolver;

    SenderExecutor(Sender sender, DelayLogFacade store, DynamicConfig sendConfig, BrokerService brokerService) {
        this.sender = sender;
        this.store = store;
        this.brokerService = brokerService;
        this.sendThreads = sendConfig.getInt("delay.send.threads", DEFAULT_SEND_THREAD);
        this.messageGroupResolver = new DefaultMessageGroupResolver(brokerService);
    }

    void execute(final List<ScheduleIndex> indexList, final GroupSender.ResultHandler handler) {
        Map<MessageGroup, List<ScheduleIndex>> messageGroups = groupMessage(indexList);
        for (Entry<MessageGroup, List<ScheduleIndex>> entry : messageGroups.entrySet()) {
            MessageGroup messageGroup = entry.getKey();
            List<ScheduleIndex> messages = entry.getValue();
            doExecute(messageGroup, messages, handler);
        }
    }

    private void doExecute(MessageGroup messageGroup, List<ScheduleIndex> messages, GroupSender.ResultHandler handler) {
        String subject = messageGroup.getSubject();
        String brokerGroupName = messageGroup.getBrokerGroup();
        BrokerClusterInfo brokerCluster = brokerService.getProducerBrokerCluster(ClientType.PRODUCER, subject);
        BrokerGroupInfo brokerGroup = brokerCluster.getGroupByName(brokerGroupName);
        GroupSender groupSender = getGroupSender(brokerGroup, sendThreads);
        groupSender.send(messageGroup, messages, sender, handler);
    }

    private Map<MessageGroup, List<ScheduleIndex>> groupMessage(final List<ScheduleIndex> indexList) {
        Map<MessageGroup, List<ScheduleIndex>> result = Maps.newHashMap();
        for (ScheduleIndex index : indexList) {
            String subject = index.getSubject();
            MessageGroup messageGroup = lookupMessageGroup(subject);
            List<ScheduleIndex> group = result.computeIfAbsent(messageGroup, k -> Lists.newArrayList());
            group.add(index);
        }

        return result;
    }

    private GroupSender getGroupSender(BrokerGroupInfo groupInfo, int sendThreads) {
        String groupName = groupInfo.getGroupName();
        GroupSender groupSender = groupSenders.get(groupName);
        if (null == groupSender) {
            groupSender = new GroupSender(groupInfo, sendThreads, store);
            GroupSender currentGroupSender = groupSenders.putIfAbsent(groupName, groupSender);
            groupSender = null != currentGroupSender ? currentGroupSender : groupSender;
        } else {
            groupSender.reconfigureGroup(groupInfo);
        }

        return groupSender;
    }

    private MessageGroup lookupMessageGroup(String subject) {
        return messageGroupResolver.resolveRandomAvailableGroup(subject, ClientType.PRODUCER);
    }

    @Override
    public void destroy() {
        groupSenders.values().parallelStream().forEach(GroupSender::destroy);
        groupSenders.clear();
    }
}
