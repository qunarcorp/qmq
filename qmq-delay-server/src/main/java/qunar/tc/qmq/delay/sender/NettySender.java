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

import qunar.tc.qmq.config.NettyClientConfigManager;
import qunar.tc.qmq.delay.store.model.ScheduleSetRecord;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.List;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-23 16:33
 */
public class NettySender implements Sender {
    private final NettyClient client;

    public NettySender() {
        this.client = NettyClient.getClient();
        this.client.start(NettyClientConfigManager.get().getDefaultClientConfig());
    }

    @Override
    public Datagram send(List<ScheduleSetRecord> records, SenderGroup senderGroup) throws InterruptedException, RemoteTimeoutException, ClientSendException {
        Datagram requestDatagram = RemotingBuilder.buildRequestDatagram(CommandCode.SEND_MESSAGE, out -> {
            if (null == records || records.isEmpty()) {
                return;
            }
            for (ScheduleSetRecord record : records) {
                out.writeBytes(record.getRecord());
            }
        });
        requestDatagram.getHeader().setVersion(RemotingHeader.VERSION_7);
        return client.sendSync(senderGroup.getBrokerGroupInfo().getMaster(), requestDatagram, 5 * 1000);
    }

    @Override
    public void shutdown() {
    }
}
