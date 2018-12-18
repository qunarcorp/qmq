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

import com.google.common.base.Preconditions;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.producer.ConfigCenter;
import qunar.tc.qmq.producer.QueueSender;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zhaohui.yu
 * 9/13/17
 */
abstract class AbstractRouterManager implements RouterManager {

    private static final ConfigCenter configs = ConfigCenter.getInstance();

    private Router router;

    private QueueSender sender;

    private final AtomicBoolean STARTED = new AtomicBoolean(false);

    @Override
    public void init(String clientId) {
        if (STARTED.compareAndSet(false, true)) {
            doInit(clientId);
            this.sender = new RPCQueueSender("qmq-sender", configs.getMaxQueueSize(), configs.getSendThreads(), configs.getSendBatch(), this);
        }
    }

    protected void doInit(String clientId) {

    }

    @Override
    public String registryOf(Message message) {
        return router.route(message).url();
    }

    void setRouter(Router router) {
        this.router = router;
    }

    @Override
    public QueueSender getSender() {
        return sender;
    }

    @Override
    public Connection routeOf(Message message) {
        Connection connection = router.route(message);
        Preconditions.checkState(connection != NopRoute.NOP_CONNECTION, "与broker连接失败，可能是配置错误，请联系TCDev");
        return connection;
    }

    @Override
    public void destroy() {
        if (sender != null) {
            sender.destroy();
        }
    }
}
