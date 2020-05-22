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

import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.protocol.RemotingHeader;

/**
 * @author keli.wang
 * @since 2018/11/2
 */
class SyncRequestEntry {
    private final ChannelHandlerContext ctx;
    private final RemotingHeader requestHeader;
    private final SyncRequest syncRequest;

    SyncRequestEntry(ChannelHandlerContext ctx, RemotingHeader requestHeader, SyncRequest syncRequest) {
        this.ctx = ctx;
        this.requestHeader = requestHeader;
        this.syncRequest = syncRequest;
    }

    ChannelHandlerContext getCtx() {
        return ctx;
    }

    RemotingHeader getRequestHeader() {
        return requestHeader;
    }

    SyncRequest getSyncRequest() {
        return syncRequest;
    }
}
