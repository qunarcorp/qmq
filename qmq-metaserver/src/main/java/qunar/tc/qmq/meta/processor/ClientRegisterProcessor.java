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

package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.meta.cache.AliveClientManager;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.cache.CachedOfflineStateManager;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.concurrent.CompletableFuture;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class ClientRegisterProcessor implements NettyRequestProcessor {

    private final ClientRegisterWorker clientRegisterWorker;
    private final AliveClientManager aliveClientManager;

    public ClientRegisterProcessor(final SubjectRouter subjectRouter,
                                   final CachedOfflineStateManager offlineStateManager,
                                   final Store store,
                                   CachedMetaInfoManager cachedMetaInfoManager) {
        this.clientRegisterWorker = new ClientRegisterWorker(subjectRouter, offlineStateManager, store, cachedMetaInfoManager);
        this.aliveClientManager = AliveClientManager.getInstance();
    }

    @Override
    public CompletableFuture<Datagram> processRequest(ChannelHandlerContext ctx, RemotingCommand command) {
        final RemotingHeader header = command.getHeader();
        final ByteBuf body = command.getBody();
        final MetaInfoRequest request = deserialize(body);
        QMon.clientRegisterCountInc(request.getSubject(), request.getClientTypeCode());

        aliveClientManager.renew(request);
        clientRegisterWorker.register(new ClientRegisterMessage(request, ctx, header));
        return null;
    }

    @SuppressWarnings("unchecked")
    private MetaInfoRequest deserialize(ByteBuf buf) {
        return new MetaInfoRequest(PayloadHolderUtils.readStringHashMap(buf));
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    static class ClientRegisterMessage {
        private final RemotingHeader header;
        private final MetaInfoRequest metaInfoRequest;
        private final ChannelHandlerContext ctx;

        ClientRegisterMessage(MetaInfoRequest metaInfoRequest, ChannelHandlerContext ctx, RemotingHeader header) {
            this.metaInfoRequest = metaInfoRequest;
            this.ctx = ctx;
            this.header = header;
        }

        RemotingHeader getHeader() {
            return header;
        }

        MetaInfoRequest getMetaInfoRequest() {
            return metaInfoRequest;
        }

        ChannelHandlerContext getCtx() {
            return ctx;
        }
    }
}
