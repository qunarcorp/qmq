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

import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.config.NettyClientConfigManager;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.producer.ConfigCenter;
import qunar.tc.qmq.protocol.Datagram;

/**
 * @author zhenyu.nie created on 2017 2017/7/4 17:49
 */
class NettyProducerClient {

    private static final ConfigCenter CONFIG = ConfigCenter.getInstance();

    private volatile boolean start = false;

    private NettyClient client;

    NettyProducerClient() {
        client = NettyClient.getClient();
    }

    public synchronized void start() {
        if (start) {
            return;
        }

        client.start(NettyClientConfigManager.get().getDefaultClientConfig());
        start = true;
    }

    Datagram sendMessage(BrokerGroupInfo group, Datagram datagram) throws InterruptedException, ClientSendException, RemoteTimeoutException {
        return client.sendSync(group.getMaster(), datagram, CONFIG.getSendTimeoutMillis());
    }


}
