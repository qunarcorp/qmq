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
import qunar.tc.qmq.configuration.DynamicConfig;
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
import java.util.concurrent.*;
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
    private static final int RETRY_FALL_BACK = 1;//refresh再来重试
    private static final int RETRY_WITHOUT_FALL_BACK = 2;//不再refresh，直接在netty端重试
    private final AtomicReference<BrokerGroupInfo> groupInfo;
    private final DelayLogFacade store;
    private DynamicConfig sendConfig;
    private final ThreadPoolExecutor executorService;
    private final RateLimiter LOG_LIMITER = RateLimiter.create(2);

    SenderGroup(final BrokerGroupInfo groupInfo, int sendThreads, DelayLogFacade store, DynamicConfig sendConfig) {
        this.sendConfig = sendConfig;
        this.groupInfo = new AtomicReference<>(groupInfo);
        this.store = store;
        final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(1);
        this.executorService = new ThreadPoolExecutor(1, sendThreads, 1L, TimeUnit.MINUTES,
                workQueue, new ThreadFactoryBuilder()
                .setNameFormat("delay-sender-" + groupInfo.getGroupName() + "-%d").build(), new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                boolean success = false;
                while (!success) {
                    try {
                        success = workQueue.offer(r);
                        //success = workQueue.add(r); 这里会频繁抛异常，微秒级别性能会有问题。
                    } catch (Throwable ignore) {

                    }
                }
            }
        });
    }

    /**
     * 绕过线程池直接执行
     * @param records
     * @param sender
     * @param handler
     */
    public void sendSync(final List<ScheduleIndex> records, final Sender sender, final ResultHandler handler) {
        monitorSyncSend(records);
        doSend(records, sender, handler, RETRY_WITHOUT_FALL_BACK);
    }

    private static void monitorSyncSend(List<ScheduleIndex> records) {
        try {
            for (int i = 0; i < records.size(); i++) {
                ScheduleIndex scheduleIndex = records.get(i);
                QMon.syncSend(scheduleIndex.getSubject());
            }
        } catch (Throwable throwable) {

        }
    }

    public void send(final List<ScheduleIndex> records, final Sender sender, final ResultHandler handler) {
        executorService.execute(() -> doSend(records, sender, handler, RETRY_FALL_BACK));
    }

    private void doSend(final List<ScheduleIndex> batch, final Sender sender, final ResultHandler handler, int retryStrategy) {
        BrokerGroupInfo groupInfo = this.groupInfo.get();
        String groupName = groupInfo.getGroupName();
        List<List<ScheduleIndex>> partitions = Lists.partition(batch, MAX_SEND_BATCH_SIZE);

        for (List<ScheduleIndex> partition : partitions) {
            send(sender, handler, groupInfo, groupName, partition, retryStrategy);
        }
    }

    private void send(Sender sender, ResultHandler handler, BrokerGroupInfo groupInfo, String groupName, List<ScheduleIndex> list, int retryStrategy) {
        try {
            long start = System.currentTimeMillis();
            List<ScheduleSetRecord> records = store.recoverLogRecord(list);
            QMon.loadMsgTime(System.currentTimeMillis() - start);

            Datagram response = sendMessages(records, sender, retryStrategy);
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

    private Datagram sendMessages(final List<ScheduleSetRecord> records, final Sender sender, int retryStrategy) {
        long start = System.currentTimeMillis();
        try {
            //return sender.send(records, this);
            return doSendDatagram(records, sender);
        } catch (ClientSendException e) {
            ClientSendException.SendErrorCode errorCode = e.getSendErrorCode();
            LOGGER.error("SenderGroup sendMessages error, client send exception, broker group={}, errorCode={}", groupInfo.get(), errorCode, e);
            monitorSendError(records, groupInfo.get(), errorCode.ordinal(), e);
        } catch (Throwable e) {
            LOGGER.error("SenderGroup sendMessages error, broker group={}", groupInfo.get(), e);
            monitorSendError(records, groupInfo.get(), -1, e);
        } finally {
            QMon.sendMsgTime(groupInfo.get().getGroupName(), System.currentTimeMillis() - start);
        }
        return retrySendMessages(records, sender, retryStrategy);
    }

    private Datagram doSendDatagram(List<ScheduleSetRecord> records, Sender sender) throws Exception {
        return sender.send(records, this);
    }

    /**
     * 如果想更优雅，可以参考guava-retry
     * @param records
     * @param sender
     * @param retryStrategy
     * @return
     */
    private Datagram retrySendMessages(List<ScheduleSetRecord> records, Sender sender, int retryStrategy) {
        Datagram result = null;
        int maxTimes = sendConfig.getInt("netty.send.message.retry.max.times", 20);
        /**
         * 如果是Refresh重试过来的，那就在这里一直重试netty
         * 如果delay机器本身出问题，refresh也没用，就在这里卡住
         */
        if (retryStrategy == RETRY_WITHOUT_FALL_BACK) {
            maxTimes = sendConfig.getInt("netty.send.message.retry.without.fallback.max.times", 36000);
        }
        for (int count = 0; count < maxTimes; count++) {
            try {
                QMon.retryNettySend();
                result = doSendDatagram(records, sender);
                if (result != null) {
                    break;
                }
            } catch (Throwable e) {
                if (LOG_LIMITER.tryAcquire()) {
                    for (ScheduleSetRecord record : records) {
                        LOGGER.error("retry senderGroup sendMessages error, retry_count={}, subject={}, brokerGroup={}", count, record.getSubject(), groupInfo.get(), e);
                    }
                }
                QMon.retryNettySendError();
                retryRandomSleep(retryStrategy);
            }
        }
        return result;
    }

    private void retryRandomSleep(int retryStrategy) {
        long randomMinSleepMillis = sendConfig.getLong("netty.send.message.retry.min.sleep.millis", 1000L);
        long randomMaxSleepMillis = sendConfig.getLong("netty.send.message.retry.max.sleep.millis", 10000L);
        if (retryStrategy == RETRY_WITHOUT_FALL_BACK) {
            randomMaxSleepMillis = sendConfig.getLong("netty.send.message.retry.without.fallback.max.sleep.millis", 30000L);
        }
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        try {
            Thread.sleep(random.nextLong(randomMinSleepMillis, randomMaxSleepMillis));
        } catch (InterruptedException ex) {

        }
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

    private void monitorSendError(List<ScheduleSetRecord> records, BrokerGroupInfo group, int errorCode, Throwable throwable) {
        for (ScheduleSetRecord record : records) {
            monitorSendError(record.getSubject(), group, errorCode, throwable);
        }
    }

    private void monitorSendError(String subject, BrokerGroupInfo group, int errorCode, Throwable throwable) {
        if (LOG_LIMITER.tryAcquire()) {
            LOGGER.error("netty delay sender send error,subject:{},group:{},code:{}", subject, group, errorCode, throwable);
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
