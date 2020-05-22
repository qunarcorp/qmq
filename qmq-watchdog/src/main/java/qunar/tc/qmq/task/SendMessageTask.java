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

package qunar.tc.qmq.task;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.task.database.DatabaseDriverMapping;
import qunar.tc.qmq.task.database.DatasourceWrapper;
import qunar.tc.qmq.task.database.IDatabaseDriver;
import qunar.tc.qmq.task.model.DataSourceInfoModel;
import qunar.tc.qmq.task.model.MsgQueue;
import qunar.tc.qmq.task.monitor.Qmon;
import qunar.tc.qmq.task.store.impl.CachedMessageClientStore;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA. User: liuzz Date: 12-12-21 Time: 上午11:15
 * <p/>
 * 用于check业务数据库里是否有未发送的消息 每个业务数据库对应一个
 */
class SendMessageTask implements Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SendMessageTask.class);

    private static final int BROKER_BUSY = -1;
    private static final int VALID_ERROR = -2;

    private static final int MAX_RETRIES = 1000;
    private static final int DEFAULT_TIME_INTEL = 60;

    private final IDatabaseDriver driver;
    private final CachedMessageClientStore messageClientStore;
    private final Gson serializer;
    private final long timeout;
    private final RateLimiter limiter;

    private final DatasourceWrapper dataSource;

    private volatile DataSourceInfoModel dataSourceInfo;
    private volatile boolean stop = true;
    private final MessageProducer messageProducer;

    SendMessageTask(DataSourceInfoModel datasourceInfo, DatabaseDriverMapping mapping, CachedMessageClientStore messageClientStore,
                    Gson serializer, long timeout, MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
        Preconditions.checkNotNull(datasourceInfo);
        Preconditions.checkNotNull(mapping);
        Preconditions.checkNotNull(messageClientStore);
        Preconditions.checkNotNull(serializer);

        this.dataSourceInfo = datasourceInfo;
        this.driver = mapping.getDatabaseMapping(dataSourceInfo.getScheme());
        this.dataSource = driver.makeDataSource(dataSourceInfo.getUrl(), datasourceInfo.getUserName(), datasourceInfo.getPassword());
        this.messageClientStore = messageClientStore;
        this.serializer = serializer;
        this.timeout = timeout;
        this.limiter = RateLimiter.create(50);
    }

    @Override
    public Void call() {
        LOG.info("{} start...", dataSourceInfo.getUrl());
        stop = false;
        String name = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(dataSourceInfo.getUrl());
            long begin = System.currentTimeMillis();
            while ((!isStop() && !timeout(begin))) {
                Date since = DateUtils.addSeconds(new Date(), -DEFAULT_TIME_INTEL);
                List<MsgQueue> errorMessages = messageClientStore.findErrorMsg(this.dataSource, since);
                if (errorMessages == null || errorMessages.isEmpty()) {
                    break;
                }
                logRemainMsg(errorMessages);
                processErrorMessages(errorMessages, since);
            }
        } catch (Exception e) {
            throw new RuntimeException("process message error, url: " + dataSourceInfo.getUrl(), e);
        } finally {
            stop = true;
            Thread.currentThread().setName(name);
            LOG.info("{} finish", dataSourceInfo.getUrl());
        }
        return null;
    }

    private boolean timeout(long begin) {
        boolean isTimeout = (System.currentTimeMillis() - begin) > timeout;
        if (isTimeout) {
            LOG.error("process db {} timeout", dataSourceInfo.getUrl());
        }
        return isTimeout;
    }

    private void processErrorMessages(List<MsgQueue> errorMessages, Date since) {
        BaseMessage message = null;
        List<BaseMessage> validMessages = new ArrayList<>();
        for (MsgQueue errorMessage : errorMessages) {
            limiter.acquire();
            if (errorMessage.updateTime.after(since)) {
                stop = true;
                break;
            }

            if (errorMessage.error >= MAX_RETRIES) {
                LOG.error("retry too many times: {}", errorMessage.id);
                error(errorMessage.id, VALID_ERROR);
                continue;
            }

            long messageId = errorMessage.id;
            String content = errorMessage.content;
            try {
                if (Strings.isNullOrEmpty(content)) {
                    error(messageId, VALID_ERROR);
                    LOG.warn("message content is empty:messageId={}", messageId);
                    continue;
                }

                try {
                    message = serializer.fromJson(content, BaseMessage.class);
                } catch (Exception e) {
                    error(messageId, VALID_ERROR);
                    LOG.warn("message deSerialize fail:messageId={},content={}", messageId, content, e);
                    continue;
                }
                if (message == null) {
                    error(messageId, VALID_ERROR);
                    LOG.warn("message deSerialize fail:messageId={},content={}", messageId, content);
                    continue;
                }

                if (!validate(message)) {
                    error(messageId, VALID_ERROR);
                    LOG.warn("message registry is empty:subject={}, messageId={}", message.getSubject(), messageId);
                    continue;
                }
                message.setProperty("qmq_sequence", messageId);
                validMessages.add(message);
            } catch (Exception e) {
                Qmon.taskSendMsgErrorCountInc(message == null ? "UNKONWN" : message.getSubject(), dataSourceInfo.getUrl());
                LOG.error("execute send task error, message content is \n{} \ndb: {}", content, dataSourceInfo.getUrl(), e);
            }
        }
        try {
            sendMessages(validMessages);
        } catch (Exception e) {
            LOG.error("send new qmq messages error. datasource: {}", dataSourceInfo.getUrl(), e);
        }
    }

    private void sendMessages(List<BaseMessage> newQmqMessages) throws InterruptedException {
        List<Long> successes = new ArrayList<>(newQmqMessages.size());
        CountDownLatch latch = new CountDownLatch(newQmqMessages.size());
        for (BaseMessage message : newQmqMessages) {
            try {
                messageProducer.sendMessage(message, new MessageSendStateListener() {
                    @Override
                    public void onSuccess(Message message) {
                        successes.add(message.getLongProperty("qmq_sequence"));
                        Qmon.sendMesssagesSuccessCountInc(message.getSubject(), dataSourceInfo.getUrl());
                        latch.countDown();
                    }

                    @Override
                    public void onFailed(Message message) {
                        LOG.warn("send message failed {}", message.getMessageId());
                        Qmon.sendNewqmqMesssagesFailedCountInc(message.getSubject(), dataSourceInfo.getUrl());
                        latch.countDown();
                    }
                });
            } catch (Exception e) {
                LOG.error("send message failed", e);
                Qmon.sendNewqmqMesssagesFailedCountInc(message.getSubject(), dataSourceInfo.getUrl());
                error(message.getLongProperty("qmq_sequence"), BROKER_BUSY);
            }
        }
        latch.await(1, TimeUnit.MINUTES);
        deleteAllSuccessMessages(successes);
    }

    private void deleteAllSuccessMessages(List<Long> successes) {
        for (Long messageId : successes) {
            try {
                messageClientStore.deleteByMessageId(dataSource, messageId);
            } catch (Exception ignore) {

            }
        }
    }

    private boolean validate(BaseMessage message) {
        String subject = message.getSubject();
        if (Strings.isNullOrEmpty(subject)) return false;
        if (subject.contains("${")) return false;
        return true;
    }

    private void error(long messageId, int state) {
        if (state == VALID_ERROR) {
            Qmon.invalidMsgCountInc(dataSourceInfo.getUrl());
            LOG.error("invalid message, db: {}", dataSourceInfo.getUrl());
        }
        messageClientStore.updateError(dataSource, messageId, state);
    }

    private void logRemainMsg(List<MsgQueue> errorMessages) {
        LOG.warn("There are remain msg: {} in client database url is {}", errorMessages.size(), dataSourceInfo.getUrl());
        Qmon.noSendMsgCountInc(dataSourceInfo.getUrl(), errorMessages.size());
    }

    void shutdown() {
        stop = true;
        messageClientStore.invalidate(this.dataSource);
        driver.close(this.dataSource);
    }

    DataSourceInfoModel getDataSourceInfo() {
        return dataSourceInfo;
    }

    void setDataSourceInfo(DataSourceInfoModel dataSourceInfo) {
        this.dataSourceInfo = dataSourceInfo;
    }

    boolean isStop() {
        return stop;
    }
}
