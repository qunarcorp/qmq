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

package qunar.tc.qmq.delay.sync.master;

import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.netty.DefaultConnectionEventHandler;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.protocol.CommandCode;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class MasterSyncNettyServer implements Disposable {
    private final NettyServer nettyServer;
    private final DelaySyncRequestProcessor syncRequestProcessor;

    public MasterSyncNettyServer(final int scale, final DynamicConfig config, final DelayLogFacade delayLogFacade) {
        final Integer port = config.getInt("sync.port", 20802);
        this.nettyServer = new NettyServer("sync", 4, port, new DefaultConnectionEventHandler("delay-sync"));
        this.syncRequestProcessor = new DelaySyncRequestProcessor(scale, delayLogFacade, config);
    }

    public void registerSyncEvent(Object listener) {
        syncRequestProcessor.registerSyncEvent(listener);
    }

    public void start() {
        nettyServer.registerProcessor(CommandCode.SYNC_LOG_REQUEST, syncRequestProcessor);
        nettyServer.start();
    }

    @Override
    public void destroy() {
        nettyServer.destroy();
    }
}
