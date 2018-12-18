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

import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.netty.DefaultConnectionEventHandler;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.store.Storage;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class MasterSyncNettyServer implements Disposable {
    private final NettyServer nettyServer;
    private final SyncLogProcessor syncLogProcessor;
    private final SyncCheckpointProcessor syncCheckpointProcessor;

    public MasterSyncNettyServer(final DynamicConfig config, final Storage storage) {
        final Integer port = config.getInt("sync.port", 20882);
        this.nettyServer = new NettyServer("sync", 4, port, new DefaultConnectionEventHandler("sync"));
        this.syncLogProcessor = new SyncLogProcessor(storage, config);
        this.syncCheckpointProcessor = new SyncCheckpointProcessor(storage);
    }

    public void registerSyncEvent(Object listener) {
        syncLogProcessor.registerSyncEvent(listener);
    }

    public void start() {
        nettyServer.registerProcessor(CommandCode.SYNC_LOG_REQUEST, syncLogProcessor);
        nettyServer.registerProcessor(CommandCode.SYNC_CHECKPOINT_REQUEST, syncCheckpointProcessor);
        nettyServer.start();
    }

    @Override
    public void destroy() {
        nettyServer.destroy();
    }
}
