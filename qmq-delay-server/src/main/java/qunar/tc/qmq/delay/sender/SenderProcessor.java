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
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.batch.BatchExecutor;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.ScheduleIndex;
import qunar.tc.qmq.delay.meta.BrokerRoleManager;
import qunar.tc.qmq.delay.store.model.DispatchLogRecord;
import qunar.tc.qmq.delay.store.model.ScheduleSetRecord;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.delay.ScheduleIndex.buildIndex;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-25 13:59
 */
public class SenderProcessor implements DelayProcessor, Processor<ByteBuf>, SenderGroup.ResultHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SenderProcessor.class);

    private static final long DEFAULT_SEND_WAIT_TIME = 1;
    private static final int DEFAULT_SEND_THREAD = 4;
    private static final int MAX_QUEUE_SIZE = 10000;
    private static final int BATCH_SIZE = 30;

    private final DynamicConfig config;
    private final SenderExecutor senderExecutor;
    private final BrokerService brokerService;
    private final DelayLogFacade facade;

    private BatchExecutor<ByteBuf> batchExecutor;

    private long sendWaitTime = DEFAULT_SEND_WAIT_TIME;

    public SenderProcessor(final DelayLogFacade facade, final BrokerService brokerService, final Sender sender, final DynamicConfig config) {
        this.brokerService = brokerService;
        this.senderExecutor = new SenderExecutor(sender, config);
        this.facade = facade;
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
    public void send(ByteBuf index) {
        if (!BrokerRoleManager.isDelayMaster()) {
            ScheduleIndex.release(index);
            return;
        }

        boolean add;
        try {
            long waitTime = Math.abs(sendWaitTime);
            if (waitTime > 0) {
                add = batchExecutor.addItem(index, waitTime, TimeUnit.MINUTES);
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
    public void process(List<ByteBuf> pureRecords) {
        if (pureRecords == null || pureRecords.isEmpty()) {
            return;
        }

        List<ScheduleSetRecord> records = null;
        try {
            records = facade.recoverLogRecord(pureRecords);
            senderExecutor.execute(records, this, brokerService);
        } catch (Exception e) {
            LOGGER.error("send message failed,messageSize:{} will retry", pureRecords.size(), e);
            retry(records);
        }
    }

    private void reject(ByteBuf record) {
        send(record);
    }

    private void success(ScheduleSetRecord record) {
        facade.appendDispatchLog(new DispatchLogRecord(record.getSubject(), record.getMessageId(), record.getScheduleTime(), record.getSequence()));
    }

    private void retry(List<ScheduleSetRecord> records, Set<String> messageIds) {
        final Set<String> refreshSubject = Sets.newHashSet();
        for (ScheduleSetRecord record : records) {
            if (messageIds.contains(record.getMessageId())) {
                refresh(record, refreshSubject);
                send(buildIndex(record.getScheduleTime(), record.getStartWroteOffset(), record.getRecordSize(), record.getSequence()));
                continue;
            }
            success(record);
        }
    }

    private void retry(List<ScheduleSetRecord> records) {
        if (null == records || records.isEmpty()) {
            return;
        }

        final Set<String> refreshSubject = Sets.newHashSet();
        for (ScheduleSetRecord record : records) {
            refresh(record, refreshSubject);
            send(buildIndex(record.getScheduleTime(), record.getStartWroteOffset(), record.getRecordSize(), record.getSequence()));
        }
    }

    private void refresh(ScheduleSetRecord record, Set<String> refreshSubject) {
        boolean refresh = !refreshSubject.contains(record.getSubject());
        if (refresh) {
            brokerService.refresh(ClientType.PRODUCER, record.getSubject());
            refreshSubject.add(record.getSubject());
        }
    }

    @Override
    public void success(List<ScheduleSetRecord> recordList, Set<String> messageIds) {
        retry(recordList, messageIds);
    }

    @Override
    public void fail(List<ScheduleSetRecord> records) {
        retry(records);
    }

    @Override
    public void destroy() {
        batchExecutor.destroy();
        senderExecutor.destroy();
    }
}
