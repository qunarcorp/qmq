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
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.BatchExecutor;
import qunar.tc.qmq.metrics.Metrics;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
class RPCQueueSender extends AbstractQueueSender {

    private BatchExecutor<ProduceMessage> executor;

    @Override
    public void init(Map<PropKey, Object> props) {
        String name = (String) Preconditions.checkNotNull(props.get(PropKey.SENDER_NAME));
        int maxQueueSize = (int) Preconditions.checkNotNull(props.get(PropKey.MAX_QUEUE_SIZE));
        int sendThreads = (int) Preconditions.checkNotNull(props.get(PropKey.SEND_THREADS));
        int sendBatch = (int) Preconditions.checkNotNull(props.get(PropKey.SEND_BATCH));

        this.routerManager = (RouterManager) Preconditions.checkNotNull(props.get(PropKey.ROUTER_MANAGER));
        this.timer = Metrics.timer("qmq_client_send_task_timer");
        this.executor = new BatchExecutor<ProduceMessage>(name, sendBatch, this);
        this.executor.setQueueSize(maxQueueSize);
        this.executor.setThreads(sendThreads);
        this.executor.init();
    }

    @Override
    public boolean offer(ProduceMessage pm) {
        return this.executor.addItem(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, long millisecondWait) {
        boolean inserted;
        try {
            inserted = this.executor.addItem(pm, millisecondWait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
        return inserted;
    }

    @Override
    public void send(ProduceMessage pm) {
        process(Arrays.asList(pm));
    }

    @Override
    public void destroy() {
        executor.destroy();
    }
}
