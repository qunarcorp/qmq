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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.delay.DelayLogFacade;
import qunar.tc.qmq.delay.ScheduleIndex;
import qunar.tc.qmq.delay.monitor.QMon;
import qunar.tc.qmq.delay.store.model.ScheduleSetRecord;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.protocol.producer.SendResult;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static qunar.tc.qmq.delay.monitor.QMon.delayBrokerSendMsgCount;
import static qunar.tc.qmq.delay.monitor.QMon.delayTime;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-16 21:01
 */
public class SenderGroup implements Disposable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SenderGroup.class);
    private static final int MAX_SEND_BATCH_SIZE = 50;
    private final AtomicReference<BrokerGroupInfo> groupInfo;
    private final DelayLogFacade store;
    private final ThreadPoolExecutor executorService;
    private final RateLimiter LOG_LIMITER = RateLimiter.create(2);

    SenderGroup(final BrokerGroupInfo groupInfo, int sendThreads, DelayLogFacade store) {
        this.groupInfo = new AtomicReference<>(groupInfo);
        this.store = store;
        this.executorService = new ThreadPoolExecutor(1, sendThreads, 1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
                .setNameFormat("delay-sender-" + groupInfo.getGroupName() + "-%d").build());
    }

    public void send(final List<ScheduleIndex> records, final Sender sender, final ResultHandler handler) {
        executorService.execute(() -> doSend(records, sender, handler));
    }

    private void doSend(final List<ScheduleIndex> batch, final Sender sender, final ResultHandler handler) {
        BrokerGroupInfo groupInfo = this.groupInfo.get();
        String groupName = groupInfo.getGroupName();
        List<List<ScheduleIndex>> partitions = Lists.partition(batch, MAX_SEND_BATCH_SIZE);

        for (List<ScheduleIndex> partition : partitions) {
            send(sender, handler, groupInfo, groupName, partition);
        }
    }

    private void send(Sender sender, ResultHandler handler, BrokerGroupInfo groupInfo, String groupName, List<ScheduleIndex> list) {
        try {
            long start = System.currentTimeMillis();
            List<ScheduleSetRecord> records = store.recoverLogRecord(list);
            QMon.loadMsgTime(System.currentTimeMillis() - start);

            Datagram response = sendMessages(records, sender);
            release(records);
            monitor(list, groupName);
            if (response == null) {
                handler.fail(list);
            } else {
                final int responseCode = response.getHeader().getCode();
                final Map<String, SendResult> resultMap = getSendResult(response);

                if (resultMap == null || responseCode != CommandCode.SUCCESS) {
                    if (responseCode == CommandCode.BROKER_REJECT || responseCode == CommandCode.BROKER_ERROR) {
                        groupInfo.markFailed();
                    }

                    monitorSendFail(list, groupInfo.getGroupName());

                    handler.fail(list);
                    return;
                }

                Set<String> failedMessageIds = new HashSet<>();
                boolean brokerRefreshed = false;
                for (Map.Entry<String, SendResult> entry : resultMap.entrySet()) {
                    int resultCode = entry.getValue().getCode();
                    if (resultCode != MessageProducerCode.SUCCESS) {
                        failedMessageIds.add(entry.getKey());
                    }
                    if (!brokerRefreshed && resultCode == MessageProducerCode.BROKER_READ_ONLY) {
                        groupInfo.markFailed();
                        brokerRefreshed = true;
                    }
                }
                if (!brokerRefreshed) groupInfo.markSuccess();

                handler.success(records, failedMessageIds);
            }
        } catch (Throwable e) {
            LOGGER.error("sender group send batch failed,broker:{},batch size:{}", groupName, list.size(), e);
            handler.fail(list);
        }
    }

    private void release(List<ScheduleSetRecord> records) {
        for (ScheduleSetRecord record : records) {
            record.release();
        }
    }

    private void monitor(final List<ScheduleIndex> indexList, final String groupName) {
        for (ScheduleIndex index : indexList) {
            String subject = index.getSubject();
            long delay = System.currentTimeMillis() - index.getScheduleTime();
            delayBrokerSendMsgCount(groupName, subject);
            delayTime(groupName, subject, delay);
        }
        Metrics.meter("delaySendMessagesQps", new String[]{"group"}, new String[]{groupName}).mark(indexList.size());
    }

    BrokerGroupInfo getBrokerGroupInfo() {
        return groupInfo.get();
    }

    void reconfigureGroup(final BrokerGroupInfo brokerGroup) {
        BrokerGroupInfo old = this.groupInfo.get();
        if (!brokerIsEquals(old, brokerGroup)) {
            this.groupInfo.set(brokerGroup);
            LOGGER.info("netty sender group reconfigure, {} -> {}", old, brokerGroup);
        }
    }

    private boolean brokerIsEquals(BrokerGroupInfo current, BrokerGroupInfo next) {
        return current.getGroupName().equals(next.getGroupName())
                && current.getMaster().equals(next.getMaster());
    }

    private Map<String, SendResult> getSendResult(Datagram response) {
        try {
            return QMQSerializer.deserializeSendResultMap(response.getBody());
        } catch (Exception e) {
            LOGGER.error("delay broker send  exception on deserializeSendResultMap.", e);
            return null;
        }
    }

    private Datagram sendMessages(final List<ScheduleSetRecord> records, final Sender sender) {
        long start = System.currentTimeMillis();

        try {
            return sender.send(records, this);
        } catch (ClientSendException e) {
            ClientSendException.SendErrorCode errorCode = e.getSendErrorCode();
            monitorSendError(records, groupInfo.get(), errorCode.ordinal());
        } catch (Exception e) {
            monitorSendError(records, groupInfo.get(), -1);
        } finally {
            QMon.sendMsgTime(groupInfo.get().getGroupName(), System.currentTimeMillis() - start);
        }

        return null;
    }

    private void monitorSendFail(List<ScheduleIndex> indexList, String groupName) {
        indexList.forEach(record -> monitorSendFail(record.getSubject(), groupName));
    }

    private void monitorSendFail(String subject, String groupName) {
        if (LOG_LIMITER.tryAcquire()) {
            LOGGER.error("netty delay sender send failed,subject:{},group:{}", subject, groupName);
        }
        QMon.nettySendMessageFailCount(subject, groupName);
    }

    private void monitorSendError(List<ScheduleSetRecord> records, BrokerGroupInfo group, int errorCode) {
        records.parallelStream().forEach(record -> monitorSendError(record.getSubject(), group, errorCode));
    }

    private void monitorSendError(String subject, BrokerGroupInfo group, int errorCode) {
        if (LOG_LIMITER.tryAcquire()) {
            LOGGER.error("netty delay sender send error,subject:{},group:{},code:{}", subject, group, errorCode);
        }
        QMon.nettySendMessageFailCount(subject, group.getGroupName());
    }

    @Override
    public void destroy() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Shutdown nettySenderExecutorService interrupted.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SenderGroup that = (SenderGroup) o;
        return Objects.equals(groupInfo.get(), that.groupInfo.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupInfo.get());
    }

    @Override
    public String toString() {
        return "SenderGroup{" +
                "groupInfo=" + groupInfo.get() +
                '}';
    }

    public interface ResultHandler {
        void success(List<ScheduleSetRecord> indexList, Set<String> messageIds);

        void fail(List<ScheduleIndex> indexList);
    }

}
