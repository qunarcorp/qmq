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

package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.meta.model.BrokerMeta;
import qunar.tc.qmq.meta.store.BrokerStore;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author keli.wang
 * @since 2017/9/26
 */
public class BrokerAcquireMetaProcessor implements NettyRequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerAcquireMetaProcessor.class);

    private final BrokerStore store;

    public BrokerAcquireMetaProcessor(final BrokerStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand command) {
        final ByteBuf body = command.getBody();
        final BrokerAcquireMetaRequest request = BrokerAcquireMetaRequestSerializer.deSerialize(body);
        final String hostname = request.getHostname();
        final int port = request.getPort();

        final String brokerAddress = hostname + "/" + port;
        LOG.info("broker request BROKER_ACQUIRE_META: {}", brokerAddress);

        final BrokerAcquireMetaResponse resp = createResponse(hostname, port);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, command.getHeader(), out -> {
            BrokerAcquireMetaResponseSerializer.serialize(resp, out);
        });

        LOG.info("assign {} to {}", resp, brokerAddress);
        return CompletableFuture.completedFuture(datagram);
    }

    private BrokerAcquireMetaResponse createResponse(final String hostname, final int servePort) {
        final BrokerMeta broker = store.queryBroker(hostname, servePort).orElseThrow(() -> new RuntimeException("cannot find broker meta for " + hostname + ":" + servePort));
        final BrokerAcquireMetaResponse resp = new BrokerAcquireMetaResponse();
        resp.setName(broker.getGroup());
        resp.setRole(broker.getRole());
        if (needSync(broker)) {
            resp.setMaster(loadSyncAddress(broker));
        } else {
            resp.setMaster("");
        }
        return resp;
    }

    private boolean needSync(final BrokerMeta broker) {
        final BrokerRole role = broker.getRole();
        return role == BrokerRole.SLAVE
                || role == BrokerRole.DELAY_SLAVE
                || role == BrokerRole.BACKUP
                || role == BrokerRole.DELAY_BACKUP;

    }

    private String loadSyncAddress(final BrokerMeta broker) {
        final List<BrokerMeta> brokers = store.queryBrokers(broker.getGroup());
        for (final BrokerMeta b : brokers) {
            if (isMaster(b)) {
                return b.getIp() + ":" + b.getSyncPort();
            }
        }

        throw new RuntimeException("cannot find master in broker group " + broker.getGroup());
    }

    private boolean isMaster(final BrokerMeta broker) {
        final BrokerRole role = broker.getRole();
        return role == BrokerRole.MASTER
                || role == BrokerRole.DELAY_MASTER;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
