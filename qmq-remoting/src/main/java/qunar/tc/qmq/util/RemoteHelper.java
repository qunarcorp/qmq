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

package qunar.tc.qmq.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class RemoteHelper {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteHelper.class);

    public static String parseChannelRemoteAddress(final Channel channel) {
        if (channel == null) {
            return "";
        }

        final SocketAddress remote = channel.remoteAddress();
        final String address = remote != null ? remote.toString() : "";

        final int index = address.lastIndexOf("/");
        if (index < 0) {
            return address;
        } else {
            return address.substring(index + 1);
        }
    }

    public static String parseSocketAddressAddress(SocketAddress address) {
        if (address == null) {
            return "";
        }

        final String addr = address.toString();
        if (addr.isEmpty()) {
            return "";
        } else {
            return addr.substring(1);
        }
    }

    public static SocketAddress string2SocketAddress(final String address) {
        final String[] s = address.split(":");
        return new InetSocketAddress(s[0], Integer.parseInt(s[1]));
    }

    public static void closeChannel(Channel channel, final boolean enableLog) {
        final String remoteAddr = RemoteHelper.parseChannelRemoteAddress(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (enableLog) {
                    LOG.info("close channel result: {}. isClosed={}", remoteAddr, future.isSuccess());
                }
            }
        });
    }
}
