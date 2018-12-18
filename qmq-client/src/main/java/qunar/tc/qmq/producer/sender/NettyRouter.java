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

import qunar.tc.qmq.Message;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.utils.DelayUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 16:29
 */
class NettyRouter implements Router {

    private final NettyProducerClient producerClient;
    private final BrokerService brokerService;

    private final ConcurrentMap<String, NettyConnection> cached;

    NettyRouter(NettyProducerClient producerClient, BrokerService brokerService) {
        this.producerClient = producerClient;
        this.brokerService = brokerService;
        this.cached = new ConcurrentHashMap<>();
    }

    @Override
    public Connection route(Message message) {
        ClientType clientType = DelayUtil.isDelayMessage(message) ? ClientType.DELAY_PRODUCER : ClientType.PRODUCER;
        String key = clientType.getCode() + "|" + message.getSubject();
        NettyConnection connection = cached.get(key);
        if (connection != null) return connection;

        connection = new NettyConnection(message.getSubject(), clientType, producerClient, brokerService);
        NettyConnection old = cached.putIfAbsent(key, connection);
        if (old == null) {
            connection.init();
            return connection;
        }
        return old;
    }
}
