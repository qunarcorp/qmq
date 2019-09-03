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
package qunar.tc.qmq.producer;


import qunar.tc.qmq.ProduceMessage;

import java.util.Map;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
public interface QueueSender {

    enum PropKey {
        SENDER_NAME,
        MAX_QUEUE_SIZE,
        SEND_THREADS,
        SEND_BATCH,
        ROUTER_MANAGER,
        ORDER_STRATEGY_MANAGER,
        BROKER_SERVICE
    }

    void init(Map<PropKey, Object> props);

    boolean offer(ProduceMessage pm);

    boolean offer(ProduceMessage pm, long millisecondWait);

    void send(ProduceMessage pm);

    void destroy();
}
