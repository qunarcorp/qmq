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

package qunar.tc.qmq.sync.master;

import com.google.common.eventbus.AsyncEventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.store.SegmentBuffer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
class MessageLogSyncWorker extends AbstractLogSyncWorker implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageLogSyncWorker.class);

    private final Storage storage;
    private final AsyncEventBus messageLogSyncEventBus;
    private final ExecutorService dispatchExecutor;

    MessageLogSyncWorker(Storage storage, DynamicConfig config) {
        super(config);
        this.storage = storage;
        this.dispatchExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("heart-event-bus"));
        this.messageLogSyncEventBus = new AsyncEventBus(dispatchExecutor);
    }

    @Override
    protected SegmentBuffer getSyncLog(SyncRequest syncRequest) {
        messageLogSyncEventBus.post(syncRequest);
        long startSyncOffset = syncRequest.getMessageLogOffset();
        long minMessageOffset = storage.getMinMessageOffset();
        if (startSyncOffset < minMessageOffset) {
            LOG.info("reset message log sync offset from {} to {}", startSyncOffset, minMessageOffset);
            startSyncOffset = minMessageOffset;
        }
        return storage.getMessageData(startSyncOffset);
    }

    void registerSyncEvent(Object listener) {
        messageLogSyncEventBus.register(listener);
    }

    @Override
    public void destroy() {
        if (dispatchExecutor != null) {
            dispatchExecutor.shutdown();
        }
    }
}
