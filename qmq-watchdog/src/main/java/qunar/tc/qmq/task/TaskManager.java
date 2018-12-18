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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.*;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.task.database.DatabaseDriverMapping;
import qunar.tc.qmq.task.model.DataSourceInfo;
import qunar.tc.qmq.task.model.DataSourceInfoModel;
import qunar.tc.qmq.task.model.DataSourceInfoStatus;
import qunar.tc.qmq.task.monitor.Qmon;
import qunar.tc.qmq.task.store.IDataSourceConfigStore;
import qunar.tc.qmq.task.store.impl.CachedMessageClientStore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class TaskManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

    private final ListeningExecutorService sendMessageExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(new NamedThreadFactory("send-message-task", true)));
    private final long sendMessageTaskExecuteTimeout;
    private final int refreshInterval;
    private final int checkInterval;
    private final String namespace;

    private final ConcurrentMap<String, SendMessageTask> sendMessageTasks = Maps.newConcurrentMap();
    private final ConcurrentMap<String, DataSourceInfoModel> dataSourceInfoMap = Maps.newConcurrentMap();

    private final CachedMessageClientStore messageClientStore;
    private final IDataSourceConfigStore dataSourceConfigStore;
    private final DatabaseDriverMapping databaseDriverMapping;
    private final MessageProducer producer;

    private final Gson serializer;

    public TaskManager(long sendMessageTaskExecuteTimeout, int refreshInterval, int checkInterval, String namespace,
                       CachedMessageClientStore messageClientStore,
                       IDataSourceConfigStore dataSourceConfigStore,
                       DatabaseDriverMapping databaseDriverMapping,
                       MessageProducer producer) {
        this.sendMessageTaskExecuteTimeout = sendMessageTaskExecuteTimeout;
        this.refreshInterval = refreshInterval;
        this.checkInterval = checkInterval;
        this.namespace = namespace;
        this.messageClientStore = messageClientStore;
        this.dataSourceConfigStore = dataSourceConfigStore;
        this.databaseDriverMapping = databaseDriverMapping;
        this.producer = producer;
        this.serializer = new Gson();
    }

    public void start() {
        initSendMessagesTasks();
        scheduleExecuteTasks();
        scheduleRefreshTask();
    }

    private void scheduleExecuteTasks() {
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("execute-send-message-task", true));
        executor.scheduleAtFixedRate(this::checkErrorMessages, checkInterval, checkInterval, TimeUnit.MILLISECONDS);
    }

    private void scheduleRefreshTask() {
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("refresh-send-message-task", true));
        executor.scheduleAtFixedRate(this::initSendMessagesTasks, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    }

    private void initSendMessagesTasks() {
        try {
            Qmon.initSendMessageTasksCountInc();
            refreshAllTasks();
        } catch (Throwable e) {
            Qmon.initSendMessageTasksFailCountInc();
            LOG.error("execute initDatasource error", e);
        }
    }

    private void refreshAllTasks() {
        final List<DataSourceInfoModel> dataSources = dataSourceConfigStore.findDataSourceInfos(DataSourceInfoStatus.ONLINE, namespace);
        LOG.info("need process db: {}", dataSources.size());
        if (dataSources.isEmpty())
            return;

        dataSourceInfoMap.clear();

        loadTasksFromDB(dataSources);

        shutdownOfflineTasks();
    }

    private void loadTasksFromDB(List<DataSourceInfoModel> dataSources) {
        try {
            for (final DataSourceInfoModel dataSource : dataSources) {
                initTask(dataSource);
            }
        } catch (Exception e) {
            LOG.error("sync db to tasks error", e);
        }
    }

    private void initTask(DataSourceInfoModel dataSource) {
        final String key = buildDataSourceKey(dataSource);
        dataSourceInfoMap.put(key, dataSource);
        if (sendMessageTasks.containsKey(key)) {
            sendMessageTasks.get(key).setDataSourceInfo(dataSource);
        } else {
            try {
                final SendMessageTask task = new SendMessageTask(dataSource, databaseDriverMapping, messageClientStore, serializer, sendMessageTaskExecuteTimeout, producer);
                sendMessageTasks.put(key, task);
            } catch (Exception e) {
                LOG.error("create task failed", e);
            }
        }
    }

    private String buildDataSourceKey(DataSourceInfo dataSource) {
        return dataSource.getUrl();
    }

    private void shutdownOfflineTasks() {
        if (sendMessageTasks.isEmpty())
            return;

        for (Map.Entry<String, SendMessageTask> entry : sendMessageTasks.entrySet()) {
            final String key = entry.getKey();
            if (!dataSourceInfoMap.containsKey(key)) {
                // 数据库没有，内存有的
                final SendMessageTask task = sendMessageTasks.remove(key);
                task.shutdown();
            }
        }
    }

    private void checkErrorMessages() {
        try {
            Qmon.sendMessageTaskInvokeCountInc();
            if (sendMessageTasks == null || sendMessageTasks.isEmpty()) {
                return;
            }

            final List<SendMessageTask> tasks = Lists.newArrayList(sendMessageTasks.values());
            if (tasks.size() == 0) {
                return;
            }
            LOG.info("send message task start total={}", tasks.size());

            final CountDownLatch latch = new CountDownLatch(tasks.size());
            final AtomicInteger failCount = new AtomicInteger();
            for (final SendMessageTask task : tasks) {
                executeTask(task, latch, failCount);
            }

            latch.await();
            LOG.info("send message task end total={}, fail={}", tasks.size(), failCount.intValue());
        } catch (Throwable t) {
            Qmon.sendMessageTaskInvokeFailCountInc();
            LOG.error("execute send message task error", t);
        }
    }

    private void executeTask(final SendMessageTask task, final CountDownLatch latch, final AtomicInteger failCount) {
        final String key = buildDataSourceKey(task.getDataSourceInfo());
        ListenableFuture<Void> future = sendMessageExecutor.submit(task);
        Futures.addCallback(future, new FutureCallback<Void>() {
            public void onSuccess(Void result) {
                LOG.info("send message task {} suc", key);
                latch.countDown();
            }

            public void onFailure(Throwable t) {
                LOG.error("send message task {} fail", key, t);
                failCount.incrementAndGet();
                latch.countDown();

            }
        });
    }

    public void destroy() {
        final List<SendMessageTask> tasks = Lists.newArrayList(sendMessageTasks.values());
        for (SendMessageTask task : tasks) {
            task.shutdown();
        }
        sendMessageExecutor.shutdown();
    }
}
