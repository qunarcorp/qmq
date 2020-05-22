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

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.sync.AbstractSyncLogProcessor;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-27 16:30
 */
public class BackupMessageLogSyncProcessor extends AbstractSyncLogProcessor {
    private final LogSyncDispatcher dispatcher;

    public BackupMessageLogSyncProcessor(final LogSyncDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void appendLogs(long startOffset, ByteBuf body) {
        dispatcher.dispatch(startOffset, body);
    }

    @Override
    public SyncRequest getRequest() {
        return new SyncRequest(dispatcher.getSyncType().getCode(), dispatcher.getSyncLogOffset(), dispatcher.getSyncLogOffset());
    }
}
