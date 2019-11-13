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

import qunar.tc.qmq.store.GroupAndPartition;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zhaohui.yu
 * 7/30/18
 */
class Subscriber {
    //3分钟无心跳，则认为暂时离线
    private static final long OFFLINE_LEASE_MILLIS = TimeUnit.MINUTES.toMillis(3);

    //2天都没有心跳则认为该consumer永久离线
    private static final long FOREVER_LEASE_MILLIS = TimeUnit.DAYS.toMillis(2);

    private final SubscriberStatusChecker checker;
    private final String name;

    private final String partitionName;
    private final String consumerGroup;
    private final String consumerId;
    private final boolean isExclusiveConsume;

    private RetryTask retryTask;
    private OfflineTask offlineTask;
    private volatile long lastUpdate;

    private final AtomicBoolean processed = new AtomicBoolean(false);

    Subscriber(SubscriberStatusChecker checker, String name, String consumerId, boolean isExclusiveConsume) {
        this.checker = checker;
        this.name = name;
        this.isExclusiveConsume = isExclusiveConsume;

        final GroupAndPartition groupAndPartition = GroupAndPartition.parse(name);
        this.consumerGroup = groupAndPartition.getGroup();
        this.partitionName = groupAndPartition.getPartitionName();
        this.consumerId = consumerId;

        this.lastUpdate = System.currentTimeMillis();
    }

    public String name() {
        return name;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public boolean isExclusiveConsume() {
        return isExclusiveConsume;
    }

    void setRetryTask(RetryTask retryTask) {
        this.retryTask = retryTask;
    }

    void setOfflineTask(OfflineTask offlineTask) {
        this.offlineTask = offlineTask;
    }

    void checkStatus() {
        try {
            Status status = status();
            if (status == Status.OFFLINE) {
                if (processed.compareAndSet(false, true)) {
                    retryTask.run();
                }
            }
            if (status == Status.FOREVER) {
                if (processed.compareAndSet(false, true)) {
                    offlineTask.run();
                }
            }
        } finally {
            processed.set(false);
        }
    }

    Status status() {
        long now = System.currentTimeMillis();
        long interval = now - lastUpdate;
        if (interval >= FOREVER_LEASE_MILLIS) {
            return Status.FOREVER;
        }
        if (interval >= OFFLINE_LEASE_MILLIS) {
            return Status.OFFLINE;
        }

        return Status.ONLINE;
    }

    void heartbeat() {
        renew();
        retryTask.cancel();
        offlineTask.cancel();
    }

    void renew() {
        lastUpdate = System.currentTimeMillis();
    }

    void reset() {
        retryTask.reset();
        offlineTask.reset();
    }

    public void destroy() {
        checker.destroy(this);
    }

    public enum Status {
        ONLINE,
        OFFLINE,
        FOREVER
    }
}
