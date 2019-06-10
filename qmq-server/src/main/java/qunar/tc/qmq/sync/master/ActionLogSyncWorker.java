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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.store.buffer.SegmentBuffer;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
class ActionLogSyncWorker extends AbstractLogSyncWorker {
    private static final Logger LOG = LoggerFactory.getLogger(ActionLogSyncWorker.class);

    private final Storage storage;

    ActionLogSyncWorker(Storage storage, DynamicConfig config) {
        super(config);
        this.storage = storage;
    }

    @Override
    protected SegmentBuffer getSyncLog(SyncRequest syncRequest) {
        long startSyncOffset = syncRequest.getActionLogOffset();
        long minActionLogOffset = storage.getMinActionLogOffset();
        if (startSyncOffset < minActionLogOffset) {
            LOG.info("reset action log sync offset from {} to {}", startSyncOffset, minActionLogOffset);
            startSyncOffset = minActionLogOffset;
        }
        return storage.getActionLogData(startSyncOffset);
    }
}
