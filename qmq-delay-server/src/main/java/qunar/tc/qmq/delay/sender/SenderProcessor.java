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

package qunar.tc.qmq.delay.sender;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.batch.BatchExecutor;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.ScheduleIndex;
import qunar.tc.qmq.delay.meta.BrokerRoleManager;
import qunar.tc.qmq.delay.store.model.DispatchLogRecord;
import qunar.tc.qmq.delay.store.model.ScheduleSetRecord;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-25 13:59
 */
public class SenderProcessor implements DelayProcessor, Processor<ScheduleIndex>, SenderGroup.ResultHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SenderProcessor.class);

    private static final long DEFAULT_SEND_WAIT_TIME = 10;
    private static final int DEFAULT_SEND_THREAD = 4;
    private static final int MAX_QUEUE_SIZE = 10000;
    private static final int BATCH_SIZE = 30;

    private final DynamicConfig config;
    private final SenderExecutor senderExecutor;
    private final BrokerService brokerService;
    private final DelayLogFacade facade;

    private BatchExecutor<ScheduleIndex> batchExecutor;

    private long sendWaitTime = DEFAULT_SEND_WAIT_TIME;

    public SenderProcessor(final DelayLogFacade store, final BrokerService brokerService, final Sender sender, final DynamicConfig config) {
        this.brokerService = brokerService;
        this.senderExecutor = new SenderExecutor(sender, store, config);
        this.facade = store;
        this.config = config;
    }

    @Override
    public void init() {
        this.batchExecutor = new BatchExecutor<>("delay-sender", BATCH_SIZE, this, DEFAULT_SEND_THREAD);
        this.batchExecutor.setQueueSize(MAX_QUEUE_SIZE);
        config.addListener(conf -> {
            this.batchExecutor.setThreads(conf.getInt("delay.send.batch.thread.size", DEFAULT_SEND_THREAD));
            this.sendWaitTime = conf.getLong("delay.send.wait.time", DEFAULT_SEND_WAIT_TIME);
        });
        this.batchExecutor.init();
    }

    @Override
    public void send(ScheduleIndex index) {
        if (!BrokerRoleManager.isDelayMaster()) {
            return;
        }

        boolean add;
        try {
            long waitTime = Math.abs(sendWaitTime);
            if (waitTime > 0) {
                add = batchExecutor.addItem(index, waitTime, TimeUnit.MILLISECONDS);
            } else {
                add = batchExecutor.addItem(index);
            }
        } catch (InterruptedException e) {
            return;
        }
        if (!add) {
            reject(index);
        }
    }

    @Override
    public void process(List<ScheduleIndex> indexList) {
        try {
            senderExecutor.execute(indexList, this, brokerService);
        } catch (Exception e) {
            LOGGER.error("send message failed,messageSize:{} will retry", indexList.size(), e);
            retry(indexList);
        }
    }

    private void reject(ScheduleIndex index) {
        send(index);
    }

    private void success(ScheduleSetRecord record) {
        facade.appendDispatchLog(new DispatchLogRecord(record.getSubject(), record.getMessageId(), record.getScheduleTime(), record.getSequence()));
    }

    private void retry(List<ScheduleSetRecord> records, Set<String> messageIds) {
        final Set<String> refreshSubject = Sets.newHashSet();
        for (ScheduleSetRecord record : records) {
            if (messageIds.contains(record.getMessageId())) {
                ScheduleIndex index = new ScheduleIndex(record.getSubject(), record.getScheduleTime(), record.getStartWroteOffset(), record.getRecordSize(), record.getSequence());
                refresh(index, refreshSubject);
                send(index);
                continue;
            }
            success(record);
        }
    }

    private void retry(List<ScheduleIndex> indexList) {
        if (null == indexList || indexList.isEmpty()) {
            return;
        }

        final Set<String> refreshSubject = Sets.newHashSet();
        for (ScheduleIndex index : indexList) {
            refresh(index, refreshSubject);
            send(index);
        }
    }

    private void refresh(ScheduleIndex index, Set<String> refreshSubject) {
        boolean refresh = !refreshSubject.contains(index.getSubject());
        if (refresh) {
            brokerService.refresh(ClientType.PRODUCER, index.getSubject());
            refreshSubject.add(index.getSubject());
        }
    }

    @Override
    public void success(List<ScheduleSetRecord> indexList, Set<String> messageIds) {
        retry(indexList, messageIds);
    }

    @Override
    public void fail(List<ScheduleIndex> indexList) {
        retry(indexList);
    }

    @Override
    public void destroy() {
        batchExecutor.destroy();
        senderExecutor.destroy();
    }
}
