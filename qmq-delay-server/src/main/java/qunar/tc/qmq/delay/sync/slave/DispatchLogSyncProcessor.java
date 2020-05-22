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

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.sync.DelaySyncRequest;
import qunar.tc.qmq.sync.SyncType;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-13 11:04
 */
public class DispatchLogSyncProcessor implements SyncLogProcessor<DelaySyncRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DispatchLogSyncProcessor.class);

    private final DelayLogFacade facade;

    DispatchLogSyncProcessor(DelayLogFacade facade) {
        this.facade = facade;
    }

    @Override
    public void process(Datagram syncData) {
        ByteBuf body = syncData.getBody();
        int size = body.readInt();
        if (size == 0) {
            LOGGER.debug("sync dispatch log data empty");
            return;
        }

        long startOffset = body.readLong();
        long baseOffset = body.readLong();
        appendLogs(startOffset, baseOffset, body);
    }

    private void appendLogs(long startOffset, long baseOffset, ByteBuf body) {
        facade.appendDispatchLogData(startOffset, baseOffset, body.nioBuffer());
    }

    @Override
    public DelaySyncRequest getRequest() {
        DelaySyncRequest.DispatchLogSyncRequest dispatchRequest = facade.getDispatchLogSyncMaxRequest();
        return new DelaySyncRequest(-1, dispatchRequest, SyncType.dispatch.getCode());
    }
}
