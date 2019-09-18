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

package qunar.tc.qmq.consumer;

import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.ConsumerGroupProgress;
import qunar.tc.qmq.store.GroupAndSubject;
import qunar.tc.qmq.store.PullLog;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by zhaohui.yu
 * 7/30/18
 */
public class SubscriberStatusChecker implements ActorSystem.Processor<Subscriber>, Runnable, Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriberStatusChecker.class);

    private final DynamicConfig config;
    private final Storage storage;
    private final ConsumerSequenceManager consumerSequenceManager;
    private final ActorSystem actorSystem;

    private final ConcurrentMap<String, ConcurrentMap<String, Subscriber>> subscribers = new ConcurrentHashMap<>();

    private volatile boolean online = false;

    private ScheduledExecutorService executor;

    public SubscriberStatusChecker(DynamicConfig config, Storage storage, ConsumerSequenceManager consumerSequenceManager) {
        this.config = config;
        this.storage = storage;
        this.consumerSequenceManager = consumerSequenceManager;
        this.actorSystem = new ActorSystem("consumers", 4, false);
    }

    public void init() {
        cleanPullLogAndCheckpoint();
        initSubscribers();
    }

    private void cleanPullLogAndCheckpoint() {
        final Table<String, String, PullLog> pullLogs = storage.allPullLogs();
        if (pullLogs == null || pullLogs.size() == 0) return;

        // delete all pull log without max pulled message sequence
        for (final String groupAndSubject : pullLogs.columnKeySet()) {
            final GroupAndSubject gs = GroupAndSubject.parse(groupAndSubject);
            final long maxPulledMessageSequence = storage.getMaxPulledMessageSequence(gs.getSubject(), gs.getGroup());
            if (maxPulledMessageSequence == -1) {
                for (final Map.Entry<String, PullLog> entry : pullLogs.column(groupAndSubject).entrySet()) {
                    final String consumerId = entry.getKey();
                    LOG.info("remove pull log. subject: {}, group: {}, consumerId: {}", gs.getSubject(), gs.getGroup(), consumerId);
                    storage.destroyPullLog(gs.getSubject(), gs.getGroup(), consumerId);
                }
            }
        }
    }

    private void initSubscribers() {
        final Collection<ConsumerGroupProgress> progresses = storage.allConsumerGroupProgresses().values();
        progresses.forEach(progress -> {
            if (progress.isExclusiveConsume()) {
                return;
            }

            progress.getConsumers().values().forEach(consumer -> {
                final String groupAndSubject = GroupAndSubject.groupAndSubject(consumer.getSubject(), consumer.getGroup());
                addSubscriber(groupAndSubject, consumer.getConsumerId());
            });
        });
    }

    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("consumer-checker"));
        executor.scheduleWithFixedDelay(this, 5, 3, TimeUnit.MINUTES);
    }

    public void brokerStatusChanged(final Boolean online) {
        LOG.info("broker online status changed from {} to {}", this.online, online);
        this.online = online;
    }

    @Override
    public void run() {
        try {
            check();
        } catch (Throwable e) {
            LOG.error("consumer status checker task failed.", e);
        }
    }

    private void check() {
        for (final ConcurrentMap<String, Subscriber> m : subscribers.values()) {
            for (final Subscriber subscriber : m.values()) {
                if (needSkipCheck()) {
                    subscriber.renew();
                } else {
                    if (subscriber.status() == Subscriber.Status.ONLINE) continue;

                    subscriber.reset();
                    actorSystem.dispatch("status-checker-" + subscriber.getGroup(), subscriber, this);
                }
            }
        }
    }

    private boolean needSkipCheck() {
        if (online) {
            return false;
        }

        return config.getBoolean("ConsumerStatusChecker.SkipCheckDuringOffline", true);
    }

    @Override
    public boolean process(Subscriber subscriber, ActorSystem.Actor<Subscriber> self) {
        subscriber.checkStatus();
        return true;
    }

    public Subscriber getSubscriber(String subject, String group, String consumerId) {
        final String groupAndSubject = GroupAndSubject.groupAndSubject(subject, group);
        final ConcurrentMap<String, Subscriber> m = subscribers.get(groupAndSubject);
        if (m == null) {
            return null;
        }

        return m.get(consumerId);
    }

    public void addSubscriber(String partitionName, String group, String consumerId) {
        final String groupAndSubject = GroupAndSubject.groupAndSubject(partitionName, group);
        addSubscriber(groupAndSubject, consumerId);
    }

    private void addSubscriber(String groupAndSubject, String consumerId) {
        ConcurrentMap<String, Subscriber> m = subscribers.get(groupAndSubject);
        if (m == null) {
            m = new ConcurrentHashMap<>();
            final ConcurrentMap<String, Subscriber> old = subscribers.putIfAbsent(groupAndSubject, m);
            if (old != null) {
                m = old;
            }
        }

        if (!m.containsKey(consumerId)) {
            m.putIfAbsent(consumerId, createSubscriber(groupAndSubject, consumerId));
        }
    }

    private Subscriber createSubscriber(String groupAndSubject, String consumerId) {
        final Subscriber subscriber = new Subscriber(this, groupAndSubject, consumerId);
        final RetryTask retryTask = new RetryTask(config, consumerSequenceManager, subscriber);
        final OfflineTask offlineTask = new OfflineTask(consumerSequenceManager, subscriber);
        subscriber.setRetryTask(retryTask);
        subscriber.setOfflineTask(offlineTask);
        return subscriber;
    }

    public void heartbeat(String consumerId, String subject, String group) {
        final String partitionName = RetryPartitionUtils.getRealPartitionName(subject);
        final String retryPartition = RetryPartitionUtils.buildRetryPartitionName(partitionName, group);

        refreshSubscriber(partitionName, group, consumerId);
        refreshSubscriber(retryPartition, group, consumerId);
    }

    private void refreshSubscriber(final String subject, final String group, final String consumerId) {
        final Subscriber subscriber = getSubscriber(subject, group, consumerId);
        if (subscriber != null) {
            subscriber.heartbeat();
        }
    }

    // TODO(keli.wang): cannot remove maxPulledMessageSequence here for now, because slave may cannot replay this correctly
    public void destroy(Subscriber subscriber) {
        final String groupAndSubject = subscriber.name();

        if (!subscribers.containsKey(groupAndSubject)) {
            return;
        }

        final ConcurrentMap<String, Subscriber> m = subscribers.get(groupAndSubject);
        if (m == null) {
            return;
        }
        m.remove(subscriber.getConsumerId());

        if (m.isEmpty()) {
            handleGroupOffline(subscriber);
        }
    }

    private void handleGroupOffline(final Subscriber lastSubscriber) {
        try {
            storage.disableLagMonitor(lastSubscriber.getSubject(), lastSubscriber.getGroup());
        } catch (Throwable e) {
            LOG.error("disable monitor error", e);
        }
        // TODO(keli.wang): how to detect group offline if master and slave's subscriber list is different
    }

    @Override
    public void destroy() {
        if (executor == null) return;
        executor.shutdown();
    }
}
