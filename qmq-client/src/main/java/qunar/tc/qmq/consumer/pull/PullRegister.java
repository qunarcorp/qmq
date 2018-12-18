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

package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.common.StatusSource;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.metainfoclient.ConsumerStateChangedListener;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static qunar.tc.qmq.common.StatusSource.*;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullRegister implements ConsumerRegister, ConsumerStateChangedListener {
    private volatile Boolean isOnline = false;

    private final Map<String, PullEntry> pullEntryMap = new HashMap<>();

    private final Map<String, DefaultPullConsumer> pullConsumerMap = new HashMap<>();

    private final ExecutorService pullExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("qmq-pull"));

    private final MetaInfoService metaInfoService;
    private final BrokerService brokerService;
    private final PullService pullService;
    private final AckService ackService;

    private String clientId;
    private String metaServer;
    private int destroyWaitInSeconds;

    public PullRegister() {
        this.metaInfoService = new MetaInfoService();
        this.brokerService = new BrokerServiceImpl(metaInfoService);
        this.pullService = new PullService();
        this.ackService = new AckService(this.brokerService);
    }

    public void init() {
        this.metaInfoService.setMetaServer(metaServer);
        this.metaInfoService.setClientId(clientId);
        this.metaInfoService.init();

        this.ackService.setDestroyWaitInSeconds(destroyWaitInSeconds);
        this.ackService.setClientId(clientId);
        this.metaInfoService.setConsumerStateChangedListener(this);
    }

    @Override
    public synchronized void regist(String subject, String group, RegistParam param) {
        registPullEntry(subject, group, param, new AlwaysPullStrategy());
        registPullEntry(RetrySubjectUtils.buildRetrySubject(subject, group), group, param, new WeightPullStrategy());
    }

    private void registPullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy) {
        final String subscribeKey = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullEntry pullEntry = pullEntryMap.get(subscribeKey);
        if (pullEntry == PullEntry.EMPTY_PULL_ENTRY) {
            throw new DuplicateListenerException(subscribeKey);
        }
        if (pullEntry == null) {
            pullEntry = createAndSubmitPullEntry(subject, group, param, pullStrategy);
        }
        if (isOnline) {
            pullEntry.online(param.getActionSrc());
        } else {
            pullEntry.offline(param.getActionSrc());
        }
    }

    private PullEntry createAndSubmitPullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy) {
        PushConsumerImpl pushConsumer = new PushConsumerImpl(subject, group, param);
        PullEntry pullEntry = new PullEntry(pushConsumer, pullService, ackService, brokerService, pullStrategy);
        pullEntryMap.put(MapKeyBuilder.buildSubscribeKey(subject, group), pullEntry);
        pullExecutor.submit(pullEntry);
        return pullEntry;
    }

    DefaultPullConsumer createDefaultPullConsumer(String subject, String group, boolean isBroadcast) {
        DefaultPullConsumer pullConsumer = new DefaultPullConsumer(subject, group, isBroadcast, clientId, pullService, ackService, brokerService);
        registerDefaultPullConsumer(pullConsumer);
        return pullConsumer;
    }

    private synchronized void registerDefaultPullConsumer(DefaultPullConsumer pullConsumer) {
        final String subscribeKey = MapKeyBuilder.buildSubscribeKey(pullConsumer.subject(), pullConsumer.group());
        if (pullEntryMap.containsKey(subscribeKey)) {
            throw new DuplicateListenerException(subscribeKey);
        }
        pullEntryMap.put(subscribeKey, PullEntry.EMPTY_PULL_ENTRY);
        pullConsumerMap.put(subscribeKey, pullConsumer);
        pullExecutor.submit(pullConsumer);
    }

    @Override
    public void unregist(String subject, String group) {
        changeOnOffline(subject, group, false, CODE);
    }

    @Override
    public void online(String subject, String group) {
        changeOnOffline(subject, group, true, OPS);
    }

    @Override
    public void offline(String subject, String group) {
        changeOnOffline(subject, group, false, OPS);
    }

    private synchronized void changeOnOffline(String subject, String group, boolean isOnline, StatusSource src) {
        final String realSubject = RetrySubjectUtils.getRealSubject(subject);
        final String retrySubject = RetrySubjectUtils.buildRetrySubject(realSubject, group);

        final String key = MapKeyBuilder.buildSubscribeKey(realSubject, group);
        final PullEntry pullEntry = pullEntryMap.get(key);
        changeOnOffline(pullEntry, isOnline, src);

        final PullEntry retryPullEntry = pullEntryMap.get(MapKeyBuilder.buildSubscribeKey(retrySubject, group));
        changeOnOffline(retryPullEntry, isOnline, src);

        final DefaultPullConsumer pullConsumer = pullConsumerMap.get(key);
        if (pullConsumer == null) return;

        if (isOnline) {
            pullConsumer.online(src);
        } else {
            pullConsumer.offline(src);
        }
    }

    private void changeOnOffline(PullEntry pullEntry, boolean isOnline, StatusSource src) {
        if (pullEntry == null) return;

        if (isOnline) {
            pullEntry.online(src);
        } else {
            pullEntry.offline(src);
        }
    }

    @Override
    public synchronized void setAutoOnline(boolean autoOnline) {
        if (autoOnline) {
            online();
        } else {
            offline();
        }
        isOnline = autoOnline;
    }

    public synchronized boolean offline() {
        isOnline = false;
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.offline(HEALTHCHECKER);
        }
        for (DefaultPullConsumer pullConsumer : pullConsumerMap.values()) {
            pullConsumer.offline(HEALTHCHECKER);
        }
        ackService.tryCleanAck();
        return true;
    }

    public synchronized boolean online() {
        isOnline = true;
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.online(HEALTHCHECKER);
        }
        for (DefaultPullConsumer pullConsumer : pullConsumerMap.values()) {
            pullConsumer.online(HEALTHCHECKER);
        }
        return true;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setMetaServer(String metaServer) {
        this.metaServer = metaServer;
    }

    @Override
    public synchronized void destroy() {
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.destroy();
        }
        ackService.destroy();
    }

    public void setDestroyWaitInSeconds(int destroyWaitInSeconds) {
        this.destroyWaitInSeconds = destroyWaitInSeconds;
    }
}
