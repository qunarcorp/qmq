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

import static qunar.tc.qmq.delay.monitor.QMon.delayBrokerSendMsgCount;
import static qunar.tc.qmq.delay.monitor.QMon.delayTime;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageGroup;
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

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-16 21:01
 */
public class GroupSender implements Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupSender.class);
    private static final int MAX_SEND_BATCH_SIZE = 50;
    private final AtomicReference<BrokerGroupInfo> brokerGroupRef;
    private final DelayLogFacade store;
    private final ThreadPoolExecutor executorService;
    private final RateLimiter LOG_LIMITER = RateLimiter.create(2);

    GroupSender(final BrokerGroupInfo brokerGroupRef, int sendThreads, DelayLogFacade store) {
        this.brokerGroupRef = new AtomicReference<>(brokerGroupRef);
        this.store = store;
        this.executorService = new ThreadPoolExecutor(1, sendThreads, 1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
                .setNameFormat("delay-sender-" + brokerGroupRef.getGroupName() + "-%d").build());
    }

    public void send(MessageGroup messageGroup, List<ScheduleIndex> records, Sender sender, ResultHandler handler) {
        executorService.execute(() -> doSend(messageGroup, records, sender, handler));
    }

    private void doSend(MessageGroup messageGroup, List<ScheduleIndex> batch, Sender sender, ResultHandler handler) {
        BrokerGroupInfo brokerGroup = this.brokerGroupRef.get();
        List<List<ScheduleIndex>> partitions = Lists.partition(batch, MAX_SEND_BATCH_SIZE);

        for (List<ScheduleIndex> partition : partitions) {
            send(messageGroup, sender, handler, brokerGroup, partition);
        }
    }

    private void send(MessageGroup messageGroup, Sender sender, ResultHandler handler, BrokerGroupInfo brokerGroup,
            List<ScheduleIndex> messages) {
        String brokerGroupName = brokerGroup.getGroupName();
        try {
            long start = System.currentTimeMillis();
            List<ScheduleSetRecord> records = store.recoverLogRecord(messages);
            QMon.loadMsgTime(System.currentTimeMillis() - start);

            Datagram response = sendMessages(messageGroup, records, sender);
            release(records);
            monitor(messages, brokerGroupName);
            if (response == null) {
                handler.fail(messages);
            } else {
                final int responseCode = response.getHeader().getCode();
                final Map<String, SendResult> resultMap = getSendResult(response);

                if (resultMap == null || responseCode != CommandCode.SUCCESS) {
                    if (responseCode == CommandCode.BROKER_REJECT || responseCode == CommandCode.BROKER_ERROR) {
                        brokerGroup.markFailed();
                    }

                    monitorSendFail(messages, brokerGroupName);

                    handler.fail(messages);
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
                        brokerGroup.markFailed();
                        brokerRefreshed = true;
                    }
                }
                if (!brokerRefreshed) {
                    brokerGroup.markSuccess();
                }

                handler.success(records, failedMessageIds);
            }
        } catch (Throwable e) {
            LOGGER.error("sender group send batch failed,broker:{},batch size:{}", brokerGroupName, messages.size(), e);
            handler.fail(messages);
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
        return brokerGroupRef.get();
    }

    void reconfigureGroup(final BrokerGroupInfo brokerGroup) {
        BrokerGroupInfo old = this.brokerGroupRef.get();
        if (!brokerIsEquals(old, brokerGroup)) {
            this.brokerGroupRef.set(brokerGroup);
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

    private Datagram sendMessages(MessageGroup messageGroup, List<ScheduleSetRecord> records, Sender sender) {
        long start = System.currentTimeMillis();

        try {
            return sender.send(messageGroup, records, this);
        } catch (ClientSendException e) {
            ClientSendException.SendErrorCode errorCode = e.getSendErrorCode();
            monitorSendError(records, brokerGroupRef.get(), errorCode.ordinal());
        } catch (Exception e) {
            monitorSendError(records, brokerGroupRef.get(), -1);
        } finally {
            QMon.sendMsgTime(brokerGroupRef.get().getGroupName(), System.currentTimeMillis() - start);
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupSender that = (GroupSender) o;
        return Objects.equals(brokerGroupRef.get(), that.brokerGroupRef.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerGroupRef.get());
    }

    @Override
    public String toString() {
        return "GroupSender{" +
                "brokerGroupRef=" + brokerGroupRef.get() +
                '}';
    }

    public interface ResultHandler {

        void success(List<ScheduleSetRecord> indexList, Set<String> messageIds);

        void fail(List<ScheduleIndex> indexList);
    }

}
