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

package qunar.tc.qmq.backup.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.store.CheckpointManager;
import qunar.tc.qmq.sync.SyncLogProcessor;
import qunar.tc.qmq.sync.SyncType;

import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.constants.BrokerConstants.DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-10 18:34
 */
public class HeartBeatProcessor implements SyncLogProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(HeartBeatProcessor.class);
    private final long sleepTimeoutMs;
    private final CheckpointManager offsetManager;

    public HeartBeatProcessor(CheckpointManager offsetManager) {
        this.offsetManager = offsetManager;
        this.sleepTimeoutMs = DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS;
    }

    @Override
    public void process(Datagram syncData) {
        try {
            TimeUnit.MILLISECONDS.sleep(sleepTimeoutMs);
        } catch (InterruptedException e) {
            LOG.error("heart beat sleep error", e);
        }
    }

    @Override
    public SyncRequest getRequest() {
        final long messageLogMaxOffset = offsetManager.getIndexCheckpointMessageOffset();
        final long actionLogMaxOffset = offsetManager.getSyncActionLogOffset();
        return new SyncRequest(SyncType.heartbeat.getCode(), messageLogMaxOffset, actionLogMaxOffset);
    }

}
