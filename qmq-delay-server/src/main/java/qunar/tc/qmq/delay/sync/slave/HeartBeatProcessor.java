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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.sync.DelaySyncRequest;
import qunar.tc.qmq.sync.SyncType;

import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.constants.BrokerConstants.DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-13 10:23
 */
public class HeartBeatProcessor implements SyncLogProcessor<DelaySyncRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatProcessor.class);

    private final DelayLogFacade facade;
    private final long sleepTimeoutMs;

    HeartBeatProcessor(DelayLogFacade facade) {
        this.facade = facade;
        this.sleepTimeoutMs = DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS;
    }

    @Override
    public void process(Datagram syncData) {
        try {
            TimeUnit.MILLISECONDS.sleep(sleepTimeoutMs);
        } catch (InterruptedException e) {
            LOGGER.error("delay slaver heart beat sleep error", e);
        }
    }

    @Override
    public DelaySyncRequest getRequest() {
        long messageLogMaxOffset = facade.getMessageLogMaxOffset();
        DelaySyncRequest.DispatchLogSyncRequest dispatchRequest = facade.getDispatchLogSyncMaxRequest();
        return new DelaySyncRequest(messageLogMaxOffset, dispatchRequest, SyncType.heartbeat.getCode());
    }
}
