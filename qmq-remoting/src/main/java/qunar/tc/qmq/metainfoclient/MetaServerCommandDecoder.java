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

package qunar.tc.qmq.metainfoclient;

import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;

import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-31.
 */
@ChannelHandler.Sharable
class MetaServerCommandDecoder extends SimpleChannelInboundHandler<Datagram> {

    interface DatagramProcessor {

        void onSuccess(Datagram datagram);
    }

    private final Map<Short, DatagramProcessor> processorMap = Maps.newConcurrentMap();

    public void registerProcessor(short requestCommandCode, DatagramProcessor processor) {
        this.processorMap.put(requestCommandCode, processor);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Datagram msg) {
        if (msg.getHeader().getCode() == CommandCode.SUCCESS) {
            short requestCode = msg.getHeader().getRequestCode();
            DatagramProcessor processor = processorMap.get(requestCode);
            if (processor != null) {
                processor.onSuccess(msg);
            }
        }
    }
}
