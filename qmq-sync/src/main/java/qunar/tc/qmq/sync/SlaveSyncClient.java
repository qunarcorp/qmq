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

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author keli.wang
 * @since 2018/10/29
 */
public class SlaveSyncClient {
    private final NettyClient client;
    private final String master;
    private volatile long timeout;

    public SlaveSyncClient(DynamicConfig config) {
        this.client = NettyClient.getClient();
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setServer(true);
        this.client.start(clientConfig);
        this.master = BrokerConfig.getMasterAddress();

        config.addListener(conf -> timeout = conf.getLong("slave.sync.timeout", 3000L));
    }

    public Datagram syncCheckpoint() {
        final Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.SYNC_CHECKPOINT_REQUEST, null);
        try {
            return client.sendSync(master, datagram, timeout);
        } catch (Throwable e) {
            throw new RuntimeException(String.format("sync checkpoint failed. master: %s, timeout: %d", master, timeout), e);
        }
    }

    public Datagram syncLog(final SyncRequest request) {
        final Datagram datagram = newSyncLogRequest(request);
        try {
            return client.sendSync(master, datagram, timeout);
        } catch (Throwable e) {
            throw new RuntimeException(String.format("sync log failed. master: %s, timeout: %d", master, timeout), e);
        }
    }

    private Datagram newSyncLogRequest(final SyncRequest request) {
        final SyncRequestPayloadHolder payloadHolder = new SyncRequestPayloadHolder(request);
        return RemotingBuilder.buildRequestDatagram(CommandCode.SYNC_LOG_REQUEST, payloadHolder);
    }

    private class SyncRequestPayloadHolder implements PayloadHolder {
        private final SyncRequest request;

        SyncRequestPayloadHolder(SyncRequest request) {
            this.request = request;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeByte(request.getSyncType());
            out.writeLong(request.getMessageLogOffset());
            out.writeLong(request.getActionLogOffset());
        }
    }
}
