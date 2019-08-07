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

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.BatchExecutor;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.MetricsConstants;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.netty.exception.SubjectNotAssignedException;
import qunar.tc.qmq.producer.QueueSender;
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
class RPCQueueSender implements QueueSender, SendErrorHandler, Processor<ProduceMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RPCQueueSender.class);

    private final BatchExecutor<ProduceMessage> executor;

    private final RouterManager routerManager;

    private final QmqTimer timer;

    public RPCQueueSender(String name, int maxQueueSize, int sendThreads, int sendBatch, RouterManager routerManager) {
        this.routerManager = routerManager;
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
    public void process(List<ProduceMessage> list) {
        long start = System.currentTimeMillis();
        try {
            //按照路由分组发送
            Collection<MessageSenderGroup> messages = groupBy(list);
            for (MessageSenderGroup group : messages) {
                QmqTimer timer = Metrics.timer("qmq_client_producer_send_broker_time");
                long startTime = System.currentTimeMillis();
                group.send();
                timer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            }
        } finally {
            timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
    }

    private Collection<MessageSenderGroup> groupBy(List<ProduceMessage> list) {
        Map<Connection, MessageSenderGroup> map = Maps.newHashMap();
        for (int i = 0; i < list.size(); ++i) {
            ProduceMessage produceMessage = list.get(i);
            produceMessage.startSendTrace();
            Connection connection = routerManager.routeOf(produceMessage.getBase());
            MessageSenderGroup group = map.get(connection);
            if (group == null) {
                group = new MessageSenderGroup(this, connection);
                map.put(connection, group);
            }
            group.addMessage(produceMessage);
        }
        return map.values();
    }

    @Override
    public void error(ProduceMessage pm, Exception e) {
        if (!(e instanceof SubjectNotAssignedException)) {
            LOGGER.warn("Message 发送失败! {}", pm.getMessageId(), e);
        }
        TraceUtil.recordEvent("error");
        pm.error(e);
    }

    @Override
    public void failed(ProduceMessage pm, Exception e) {
        LOGGER.warn("Message 发送失败! {}", pm.getMessageId(), e);
        TraceUtil.recordEvent("failed ");
        pm.failed();
    }

    @Override
    public void block(ProduceMessage pm, MessageException ex) {
        LOGGER.warn("Message 发送失败! {},被server拒绝,请检查应用授权配置,如果需要恢复消息请手工到db恢复状态", pm.getMessageId(), ex);
        TraceUtil.recordEvent("block");
        pm.block();
    }

    @Override
    public void finish(ProduceMessage pm, Exception e) {
        LOGGER.info("发送成功 {}:{}", pm.getSubject(), pm.getMessageId());
        pm.finish();
    }

    @Override
    public void destroy() {
        executor.destroy();
    }
}
