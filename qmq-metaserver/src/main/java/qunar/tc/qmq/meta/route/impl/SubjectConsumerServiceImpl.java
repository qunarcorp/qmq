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

import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.model.GroupedConsumer;
import qunar.tc.qmq.meta.route.SubjectConsumerService;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;
import qunar.tc.qmq.meta.store.impl.ClientMetaInfoStoreImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2017/12/5
 */
public class SubjectConsumerServiceImpl implements SubjectConsumerService {
    private static final String NEW_QMQ_NAMESPACE = "newqmq";

    private final ClientMetaInfoStore clientMetaInfoStore = new ClientMetaInfoStoreImpl();

    @Override
    public List<GroupedConsumer> consumers(final String subject) {
        final List<ClientMetaInfo> metaInfos = clientMetaInfoStore.queryConsumer(subject);
        final Map<String, GroupedConsumer> groupedConsumers = new HashMap<>();
        for (ClientMetaInfo clientMetaInfo : metaInfos) {
            GroupedConsumer groupedConsumer = groupedConsumers.get(clientMetaInfo.getConsumerGroup());
            if (groupedConsumer == null) {
                groupedConsumer = new GroupedConsumer(NEW_QMQ_NAMESPACE, subject, clientMetaInfo.getConsumerGroup());
                groupedConsumers.put(clientMetaInfo.getConsumerGroup(), groupedConsumer);
            }
            List<String> endpoints = groupedConsumer.getEndPoint();
            if (endpoints == null) {
                endpoints = new ArrayList<>();
                groupedConsumer.setEndPoint(endpoints);
            }
            endpoints.add(buildNewQmqEndPoint(clientMetaInfo));
        }
        return new ArrayList<>(groupedConsumers.values());
    }

    private String buildNewQmqEndPoint(ClientMetaInfo clientMetaInfo) {
        return "qmq://" +
                clientMetaInfo.getClientId() +
                "?" +
                "room=" +
                clientMetaInfo.getRoom() +
                "&appCode=" +
                clientMetaInfo.getAppCode();
    }
}
