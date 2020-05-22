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

package qunar.tc.qmq.sync;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.Datagram;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public abstract class AbstractSyncLogProcessor implements SyncLogProcessor {

    @Override
    public void process(Datagram datagram) {
        final ByteBuf body = datagram.getBody();
        body.readInt();
        final long startOffset = body.readLong();
        appendLogs(startOffset, body);
    }

    public abstract void appendLogs(long startOffset, ByteBuf body);
}
