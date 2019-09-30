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

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public interface BrokerService extends ClientMetaManager {

    BrokerClusterInfo getProducerBrokerCluster(ClientType clientType, String subject);

    BrokerClusterInfo getConsumerBrokerCluster(ClientType clientType, String subject);

    BrokerClusterInfo getBrokerCluster(ClientType clientType, String subject, String group, boolean isBroadcast, boolean isOrdered);

    void refresh(ClientType clientType, String subject);

    void refresh(ClientType clientType, String subject, String consumerGroup);

    void releaseLock(String subject, String consumerGroup, String partitionName, String brokerGroupName, ConsumeStrategy consumeStrategy);

    String getAppCode();
}
