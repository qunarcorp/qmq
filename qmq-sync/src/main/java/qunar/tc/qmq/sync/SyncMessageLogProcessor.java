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
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.store.Storage;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class SyncMessageLogProcessor extends AbstractSyncLogProcessor {

    private final Storage storage;

    public SyncMessageLogProcessor(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void appendLogs(long startOffset, ByteBuf body) {
        storage.appendMessageLogData(startOffset, body.nioBuffer());
    }

    @Override
    public SyncRequest getRequest() {
        final long messageLogMaxOffset = storage.getMaxMessageOffset();
        return new SyncRequest(SyncType.message.getCode(), messageLogMaxOffset, 0L);
    }
}
