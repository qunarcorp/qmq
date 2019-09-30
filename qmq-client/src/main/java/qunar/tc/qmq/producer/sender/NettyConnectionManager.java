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

package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.netty.client.NettyClient;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 16:29
 */
public class NettyConnectionManager implements ConnectionManager {

    private final NettyClient client;
    private final BrokerService brokerService;

    private final ConcurrentMap<MessageGroup, NettyConnection> cached;

    public NettyConnectionManager(NettyClient client, BrokerService brokerService) {
        this.client = client;
        this.brokerService = brokerService;
        this.cached = new ConcurrentHashMap<>();
    }

    @Override
    public Connection getConnection(MessageGroup messageGroup) {
        NettyConnection connection = cached.get(messageGroup);
        if (connection != null) return connection;

        String subject = messageGroup.getSubject();
        ClientType clientType = messageGroup.getClientType();
        String brokerGroup = messageGroup.getBrokerGroup();
        BrokerClusterInfo cluster = brokerService.getProducerBrokerCluster(clientType, subject);
        connection = new NettyConnection(subject, clientType, client, brokerService, cluster.getGroupByName(brokerGroup));
        NettyConnection old = cached.putIfAbsent(messageGroup, connection);
        if (old == null) {
            connection.init();
            return connection;
        }
        return old;
    }
}
