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

package qunar.tc.qmq.sync;

import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.Datagram;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public class SlaveSyncSender {
    private final NettyClient client;
    private final long timeout;

    public SlaveSyncSender(DynamicConfig config, NettyClient client) {
        this.timeout = config.getLong("slave.sync.timeout", 3000L);
        this.client = client;
    }

    public Datagram send(String address, Datagram datagram) throws InterruptedException, RemoteTimeoutException, ClientSendException {
        return client.sendSync(address, datagram, timeout);
    }
}
