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

package qunar.tc.qmq.meta;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.utils.NetworkUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2017/9/1
 */
public class BrokerRegisterService implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerRegisterService.class);

    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(2);
    private static final int HEARTBEAT_DELAY_SECONDS = 10;

    private final ScheduledExecutorService heartbeatScheduler;
    private final MetaServerLocator locator;
    private final NettyClient client;
    private final int port;
    private final String brokerAddress;

    private volatile int brokerState;
    private volatile String endpoint;

    public BrokerRegisterService(final int port, final MetaServerLocator locator) {
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("broker-register-heartbeat"));
        this.locator = locator;
        this.client = NettyClient.getClient();
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(HEARTBEAT_DELAY_SECONDS * 2);
        config.setClientWorkerThreads(1);
        this.client.start(config);
        this.port = port;
        this.brokerAddress = BrokerConfig.getBrokerAddress() + ":" + port;
        this.brokerState = BrokerState.NRW.getCode();

        repickEndpoint();
    }

    public void start() {
        acquireMeta();
        heartbeatScheduler.scheduleWithFixedDelay(this::heartbeat, 0, HEARTBEAT_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    private void acquireMeta() {
        Datagram datagram = null;
        try {
            datagram = client.sendSync(endpoint, buildAcquireMetaDatagram(), TIMEOUT_MS);
            final BrokerAcquireMetaResponse meta = BrokerAcquireMetaResponseSerializer.deSerialize(datagram.getBody());
            BrokerConfig.getInstance().updateMeta(meta);
        } catch (Exception e) {
            LOG.error("Send acquire meta message to meta server failed", e);
            throw new RuntimeException(e);
        } finally {
            if (datagram != null) {
                datagram.release();
            }
        }
    }

    private void heartbeat() {
        try {
            Datagram datagram = null;
            try {
                datagram = client.sendSync(endpoint, buildRegisterDatagram(BrokerRequestType.HEARTBEAT), TIMEOUT_MS);
            } catch (Exception e) {
                LOG.error("Send HEARTBEAT message to meta server failed", e);
                repickEndpoint();
            } finally {
                if (datagram != null) {
                    datagram.release();
                }
            }
        }catch (Exception e){
            LOG.error("Heartbeat error", e);
        }
    }

    public void healthSwitch(final Boolean online) {
        if (online) {
            brokerOnline();
        } else {
            brokerOffline();
        }
    }

    private void brokerOnline() {
        Datagram datagram = null;
        try {
            brokerState = BrokerState.RW.getCode();
            datagram = client.sendSync(endpoint, buildRegisterDatagram(BrokerRequestType.ONLINE), TIMEOUT_MS);
        } catch (Exception e) {
            LOG.error("Send ONLINE message to meta server failed", e);
            repickEndpoint();
            throw new RuntimeException("broker online failed", e);
        } finally {
            if (datagram != null) {
                datagram.release();
            }
        }
    }

    private void brokerOffline() {
        Datagram datagram = null;
        try {
            brokerState = BrokerState.NRW.getCode();
            datagram = client.sendSync(endpoint, buildRegisterDatagram(BrokerRequestType.OFFLINE), TIMEOUT_MS);
        } catch (Exception e) {
            LOG.error("Send OFFLINE message to meta server failed", e);
            repickEndpoint();
            throw new RuntimeException("broker offline failed", e);
        } finally {
            if (datagram != null) {
                datagram.release();
            }
        }
    }

    private void repickEndpoint() {
        Optional<String> optional = locator.queryEndpoint();
        if (optional.isPresent()) {
            this.endpoint = optional.get();
        }
    }

    private Datagram buildAcquireMetaDatagram() {
        final Datagram datagram = new Datagram();
        final RemotingHeader header = new RemotingHeader();
        header.setCode(CommandCode.BROKER_ACQUIRE_META);
        datagram.setHeader(header);
        datagram.setPayloadHolder(out -> {
            final BrokerAcquireMetaRequest request = new BrokerAcquireMetaRequest();
            request.setHostname(NetworkUtils.getLocalHostname());
            request.setPort(port);
            BrokerAcquireMetaRequestSerializer.serialize(request, out);
        });
        return datagram;
    }

    private Datagram buildRegisterDatagram(final BrokerRequestType checkType) {
        final Datagram datagram = new Datagram();
        final RemotingHeader header = new RemotingHeader();
        header.setCode(CommandCode.BROKER_REGISTER);
        datagram.setHeader(header);
        datagram.setPayloadHolder(out -> {
            final BrokerRegisterRequest request = buildRegisterRequest(checkType);
            BrokerRegisterRequestSerializer.serialize(request, out);
        });
        return datagram;
    }

    private BrokerRegisterRequest buildRegisterRequest(final BrokerRequestType checkType) {
        final BrokerRegisterRequest request = new BrokerRegisterRequest();
        request.setGroupName(BrokerConfig.getBrokerName());
        request.setBrokerRole(BrokerConfig.getBrokerRole().getCode());
        request.setBrokerState(brokerState);
        request.setRequestType(checkType.getCode());
        request.setBrokerAddress(brokerAddress);
        return request;
    }

    @Override
    public void destroy() {
        heartbeatScheduler.shutdown();
        try {
            heartbeatScheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Shutdown heartbeat scheduler interrupted.");
        }
    }
}
