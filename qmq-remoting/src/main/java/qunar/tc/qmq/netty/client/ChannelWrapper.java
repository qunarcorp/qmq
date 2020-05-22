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

package qunar.tc.qmq.netty.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author yiqun.fan create on 17-8-29.
 */
class ChannelWrapper {
    private final ChannelFuture channelFuture;

    ChannelWrapper(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    ChannelFuture getChannelFuture() {
        return channelFuture;
    }

    Channel getChannel() {
        return this.channelFuture.channel();
    }

    boolean isOK() {
        return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
    }
}
