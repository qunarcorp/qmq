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

package qunar.tc.qmq.producer.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 14:21
 */
public class NettyRouterManager extends AbstractRouterManager {
    private static final Logger logger = LoggerFactory.getLogger(NettyRouterManager.class);

    private static final int _32K = (32 * 1024) / 4;

    private final MetaInfoService metaInfoService;
    private final BrokerService brokerService;

    private String metaServer;
    private String appCode;

    public NettyRouterManager() {
        this.metaInfoService = new MetaInfoService();
        this.brokerService = new BrokerServiceImpl(this.metaInfoService);
    }

    @Override
    public void doInit(String clientId) {
        this.brokerService.setAppCode(appCode);

        this.metaInfoService.setMetaServer(metaServer);
        this.metaInfoService.setClientId(clientId);
        this.metaInfoService.init();

        NettyProducerClient producerClient = new NettyProducerClient();
        producerClient.start();
        setRouter(new NettyRouter(producerClient, this.brokerService));
    }

    @Override
    public String name() {
        return "netty";
    }

    @Override
    public void validateMessage(Message message) {
        Map<String, Object> attrs = message.getAttrs();
        if (attrs == null) return;
        for (Map.Entry<String, Object> entry : attrs.entrySet()) {
            if (entry.getValue() == null) return;
            if (!(entry.getValue() instanceof String)) return;

            String value = (String) entry.getValue();
            if (value.length() > _32K) {
                TraceUtil.recordEvent("big_message");
                String msg = entry.getKey() + "的value长度超过32K，请使用Message.setLargeString方法设置，并且使用Message.getLargeString方法获取";
                logger.error(msg, new RuntimeException());
            }
        }
    }

    public void setMetaServer(String metaServer) {
        this.metaServer = metaServer;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }
}
