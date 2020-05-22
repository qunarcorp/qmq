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
 * @since 2018-08-13 10:52
 */
public class MessageLogSyncProcessor implements SyncLogProcessor<DelaySyncRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageLogSyncProcessor.class);

    private final DelayLogFacade facade;

    MessageLogSyncProcessor(DelayLogFacade facade) {
        this.facade = facade;
    }

    @Override
    public void process(Datagram syncData) {
        final ByteBuf body = syncData.getBody();
        final int size = body.readInt();
        if (size == 0) {
            LOGGER.debug("sync message log data empty");
            return;
        }
        final long startOffset = body.readLong();
        appendLogs(startOffset, body);
    }

    private void appendLogs(long startOffset, ByteBuf body) {
        facade.appendMessageLogData(startOffset, body.nioBuffer());
    }

    @Override
    public DelaySyncRequest getRequest() {
        long messageLogMaxOffset = facade.getMessageLogMaxOffset();
        return new DelaySyncRequest(messageLogMaxOffset, null, SyncType.message.getCode());
    }
}
