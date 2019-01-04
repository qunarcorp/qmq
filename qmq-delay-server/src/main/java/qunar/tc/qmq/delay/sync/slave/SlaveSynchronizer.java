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

package qunar.tc.qmq.delay.sync.slave;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.MetricsConstants;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.sync.DelaySyncRequest;
import qunar.tc.qmq.sync.SlaveSyncSender;
import qunar.tc.qmq.sync.SyncType;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-13 10:12
 */
public class SlaveSynchronizer implements Disposable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlaveSynchronizer.class);

    private static final RateLimiter LOG_LIMITER = RateLimiter.create(0.2);

    private final String masterAddress;
    private final SlaveSyncSender slaveSyncSender;
    private final Map<SyncType, SlaveSyncTask> slaveSyncTasks;

    public SlaveSynchronizer(final String masterAddress, final DynamicConfig config, final DelayLogFacade facade) {
        this.masterAddress = masterAddress;
        final NettyClient client = NettyClient.getClient();
        client.start(new NettyClientConfig());
        this.slaveSyncSender = new SlaveSyncSender(config, client);
        this.slaveSyncTasks = new HashMap<>();

        SyncLogProcessor<DelaySyncRequest> messageLogSyncProcessor = new MessageLogSyncProcessor(facade);
        SlaveSyncTask messageLogSyncTask = new SlaveSyncTask(messageLogSyncProcessor);
        this.slaveSyncTasks.put(SyncType.message, messageLogSyncTask);

        SyncLogProcessor<DelaySyncRequest> dispatchLogSyncProcessor = new DispatchLogSyncProcessor(facade);
        SlaveSyncTask dispatchLogSyncTask = new SlaveSyncTask(dispatchLogSyncProcessor);
        this.slaveSyncTasks.put(SyncType.dispatch, dispatchLogSyncTask);

        SyncLogProcessor<DelaySyncRequest> heartBeatProcessor = new HeartBeatProcessor(facade);
        SlaveSyncTask heartBeatTask = new SlaveSyncTask(heartBeatProcessor);
        this.slaveSyncTasks.put(SyncType.heartbeat, heartBeatTask);
    }

    public void startSync() {
        for (final Map.Entry<SyncType, SlaveSyncTask> entry : slaveSyncTasks.entrySet()) {
            new Thread(entry.getValue(), "delay-sync-task-" + entry.getKey().name()).start();
            LOGGER.info("slave {} synchronizer started. ", entry.getKey());
        }
    }

    @Override
    public void destroy() {
        slaveSyncTasks.forEach((syncType, slaveSyncTask) -> slaveSyncTask.shutdown());
    }

    private class SlaveSyncTask implements Runnable {
        private final SyncLogProcessor<DelaySyncRequest> processor;
        private final String processorName;

        private volatile boolean running = true;

        SlaveSyncTask(SyncLogProcessor<DelaySyncRequest> processor) {
            this.processor = processor;
            this.processorName = processor.getClass().getSimpleName();
        }

        void shutdown() {
            running = false;
        }

        @Override
        public void run() {
            final long start = System.currentTimeMillis();
            while (running) {
                try {
                    sync();
                } catch (Throwable e) {
                    if (LOG_LIMITER.tryAcquire()) {
                        LOGGER.error("sync data from master error", e);
                    }
                } finally {
                    final QmqTimer timer = Metrics.timer("syncTaskExecTimer", MetricsConstants.PROCESSOR, new String[]{processorName});
                    timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                }
            }
        }

        private void sync() throws InterruptedException, RemoteTimeoutException, ClientSendException {
            syncFromMaster(newSyncRequest());
        }

        private Datagram newSyncRequest() {
            final DelaySyncRequest request = processor.getRequest();
            final SyncRequestPayloadHolder holder = new SyncRequestPayloadHolder(request);
            return RemotingBuilder.buildRequestDatagram(CommandCode.SYNC_LOG_REQUEST, holder);
        }

        private void syncFromMaster(Datagram request) throws InterruptedException, RemoteTimeoutException, ClientSendException {
            Datagram response = null;
            try {
                response = slaveSyncSender.send(masterAddress, request);
                processor.process(response);
            } finally {
                if (response != null) {
                    response.release();
                }
            }
        }
    }

    private class SyncRequestPayloadHolder implements PayloadHolder {
        private final DelaySyncRequest request;

        SyncRequestPayloadHolder(DelaySyncRequest request) {
            this.request = request;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeByte(request.getSyncType());
            out.writeLong(request.getMessageLogOffset());
            out.writeLong(request.getDispatchSegmentBaseOffset());
            out.writeLong(request.getDispatchLogOffset());
            out.writeLong(request.getLastDispatchSegmentBaseOffset());
            out.writeLong(request.getLastDispatchSegmentOffset());
        }
    }
}
