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

package qunar.tc.qmq.delay.sync.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.store.SegmentBuffer;
import qunar.tc.qmq.sync.DelaySyncRequest;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yunfeng.yang
 * @since 2017/8/21
 */
abstract class AbstractLogSyncWorker implements DelaySyncRequestProcessor.SyncProcessor {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractLogSyncWorker.class);
    protected final DynamicConfig config;
    final DelayLogFacade delayLogFacade;
    private final AtomicBoolean TIMER_DEFINED = new AtomicBoolean(false);
    ScheduledExecutorService processorTimer;

    AbstractLogSyncWorker(DelayLogFacade delayLogFacade, DynamicConfig config) {
        this.delayLogFacade = delayLogFacade;
        this.config = config;
    }

    @Override
    public void process(DelaySyncRequestProcessor.SyncRequestEntry entry) {
        final DelaySyncRequest delaySyncRequest = entry.getDelaySyncRequest();
        final SegmentBuffer result = getSyncLog(delaySyncRequest);
        if (result == null || result.getSize() <= 0) {
            LOGGER.debug("log sync process empty result {}, {}", result, delaySyncRequest);
            newTimeout(new DelaySyncRequestProcessor.SyncRequestTimeoutTask(entry, this));
            return;
        }

        processSyncLog(entry, result);
    }

    void defineTimer(ScheduledExecutorService processorTimer) {
        if (TIMER_DEFINED.compareAndSet(false, true)) {
            this.processorTimer = processorTimer;
            return;
        }

        LOGGER.error("message log sync worker timer defined more than once");
    }

    @Override
    public void processTimeout(DelaySyncRequestProcessor.SyncRequestEntry entry) {
        final DelaySyncRequest syncRequest = entry.getDelaySyncRequest();
        final SegmentBuffer result = getSyncLog(syncRequest);

        if (result == null || result.getSize() <= 0) {
            entry.getCtx().writeAndFlush(resolveResult(syncRequest, entry.getRequestHeader()));
            return;
        }

        processSyncLog(entry, result);
    }

    protected abstract void newTimeout(final DelaySyncRequestProcessor.SyncRequestTimeoutTask task);

    protected abstract Datagram resolveResult(final DelaySyncRequest syncRequest, final RemotingHeader header);

    protected abstract void processSyncLog(final DelaySyncRequestProcessor.SyncRequestEntry entry, final SegmentBuffer result);

    protected abstract SegmentBuffer getSyncLog(final DelaySyncRequest delaySyncRequest);
}
