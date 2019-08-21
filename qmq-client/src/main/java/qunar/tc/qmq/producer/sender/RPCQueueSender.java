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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ErrorHandler;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.BatchExecutor;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.netty.exception.SubjectNotAssignedException;
import qunar.tc.qmq.producer.SendErrorHandler;
import qunar.tc.qmq.service.exceptions.MessageException;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
class RPCQueueSender extends AbstractQueueSender implements Processor<ProduceMessage>, SendErrorHandler {

    private static final Logger logger = LoggerFactory.getLogger(RPCQueueSender.class);

    private BatchExecutor<ProduceMessage> executor;
    protected QmqTimer timer;

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

    @Override
    public void error(ProduceMessage pm, Exception e) {
        if (!(e instanceof SubjectNotAssignedException)) {
            logger.warn("Message 发送失败! {}", pm.getMessageId(), e);
        }
        TraceUtil.recordEvent("error");
        pm.error(e);
    }

    @Override
    public void failed(ProduceMessage pm, Exception e) {
        logger.warn("Message 发送失败! {}", pm.getMessageId(), e);
        TraceUtil.recordEvent("failed ");
        pm.failed();
    }

    @Override
    public void block(ProduceMessage pm, MessageException ex) {
        logger.warn("Message 发送失败! {},被server拒绝,请检查应用授权配置,如果需要恢复消息请手工到db恢复状态", pm.getMessageId(), ex);
        TraceUtil.recordEvent("block");
        pm.block();
    }

    @Override
    public void finish(ProduceMessage pm, Exception e) {
        logger.info("发送成功 {}:{}", pm.getSubject(), pm.getMessageId());
        pm.finish();
    }

    @Override
    public void postHandle(List<ProduceMessage> sourceMessages) {

    }

    @Override
    public void process(List<ProduceMessage> produceMessages) {
        long start = System.currentTimeMillis();
        try {
            //按照路由分组发送
            Collection<MessageSenderGroup> messages = groupBy(produceMessages);
            for (MessageSenderGroup group : messages) {
                QmqTimer timer = Metrics.timer("qmq_client_producer_send_broker_time");
                long startTime = System.currentTimeMillis();
                group.send(this);
                timer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            }
        } finally {
            timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
    }
}
