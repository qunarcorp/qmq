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

package qunar.tc.qmq.processor;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.netty.ConnectionEventHandler;
import qunar.tc.qmq.stats.BrokerStats;

/**
 * @author keli.wang
 * @since 2018/7/18
 */
public class BrokerConnectionEventHandler implements ConnectionEventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerConnectionEventHandler.class);

    private final BrokerStats brokerStats = BrokerStats.getInstance();

    public BrokerConnectionEventHandler() {
        QMon.activeClientCount(() -> brokerStats.getActiveClientConnectionCount().doubleValue());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOG.info("client {} connected", ctx.channel().remoteAddress());
        brokerStats.getActiveClientConnectionCount().incrementAndGet();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOG.info("client {} disconnected", ctx.channel().remoteAddress());
        brokerStats.getActiveClientConnectionCount().decrementAndGet();
    }
}
