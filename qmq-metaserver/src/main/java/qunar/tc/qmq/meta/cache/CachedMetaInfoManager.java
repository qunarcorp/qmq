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

package qunar.tc.qmq.meta.cache;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerGroupKind;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.meta.model.ReadonlyBrokerGroupSetting;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.order.PartitionService;
import qunar.tc.qmq.meta.store.ReadonlyBrokerGroupSettingStore;
import qunar.tc.qmq.meta.store.Store;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class CachedMetaInfoManager implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(CachedMetaInfoManager.class);

    private static final ScheduledExecutorService SCHEDULE_POOL = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("meta-info-refresh-%d").build());
    private static final long DEFAULT_REFRESH_PERIOD_SECONDS = 5L;

    private static final String DEFAULT_BROKER_GROUP_TAG = "_default_";

    private final Store store;
    private final ReadonlyBrokerGroupSettingStore readonlyBrokerGroupSettingStore;
    private final PartitionService partitionService;
    private final long refreshPeriodSeconds;

    /**
     * subject -> groupNames
     */
    private volatile Map<String, List<String>> cachedSubjectGroups = new HashMap<>();
    /**
     * groupName -> brokerGroup
     */
    private volatile Map<String, BrokerGroup> cachedBrokerGroups = new HashMap<>();
    /**
     * tag -> groupNames
     */
    private volatile Map<String, List<String>> cachedTagToBrokerGroups = new HashMap<>();
    /**
     * groupName -> brokerNewGroup
     */
    private volatile Map<String, BrokerGroup> cachedDelayNewGroups = new HashMap<>();
    /**
     * subjectName -> subjectInfo
     */
    private volatile Map<String, SubjectInfo> cachedSubjectInfoMap = new HashMap<>();

    /**
     * subject -> partitionMapping
     */
    private volatile Map<String, ProducerAllocation> cachedProducerAllocations = new HashMap<>();

    /**
     * brokerGroupName -> Subject List
     */
    private volatile SetMultimap<String, String> readonlyBrokerGroupSettings = HashMultimap.create();

    public CachedMetaInfoManager(DynamicConfig config, Store store, ReadonlyBrokerGroupSettingStore readonlyBrokerGroupSettingStore, PartitionService partitionService) {
        this.refreshPeriodSeconds = config.getLong("refresh.period.seconds", DEFAULT_REFRESH_PERIOD_SECONDS);
        this.store = store;
        this.readonlyBrokerGroupSettingStore = readonlyBrokerGroupSettingStore;
        this.partitionService = partitionService;
        refresh();
        initRefreshTask();
    }

    public SubjectInfo getSubjectInfo(String subject) {
        return cachedSubjectInfoMap.get(subject);
    }

    private void initRefreshTask() {
        SCHEDULE_POOL.scheduleAtFixedRate(new RefreshTask(), refreshPeriodSeconds, refreshPeriodSeconds, TimeUnit.SECONDS);
    }

    public List<String> getGroups(String subject) {
        final List<String> groups = cachedSubjectGroups.get(subject);
        if (groups == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(groups);
    }

    public List<String> getAllBrokerGroupNamesByTag(String tag) {
        if (tag == null) {
            return ImmutableList.of();
        }
        List<String> groupNames = cachedTagToBrokerGroups.get(tag);
        return groupNames == null ? ImmutableList.of() : ImmutableList.copyOf(groupNames);
    }

    public List<String> getAllDefaultTagBrokerGroupNames() {
        return ImmutableList.copyOf(cachedTagToBrokerGroups.get(DEFAULT_BROKER_GROUP_TAG));
    }

    public BrokerGroup getBrokerGroup(String groupName) {
        return cachedBrokerGroups.get(groupName);
    }

    public List<BrokerGroup> getDelayNewGroups() {
        return Lists.newArrayList(cachedDelayNewGroups.values());
    }

    public void executeRefreshTask() {
        try {
            SCHEDULE_POOL.execute(new RefreshTask());
        } catch (Exception e) {
            LOG.error("execute refresh task reject");
        }
    }

    private void refresh() {
        LOG.info("refresh meta info");
        refreshBrokerGroups();
        refreshSubjectInfoCache();
        refreshGroupsAndSubjects();
        refreshReadonlyBrokerGroupSettings();
        refreshPartitionMapping();
    }

    private void refreshReadonlyBrokerGroupSettings() {
        final HashMultimap<String, String> map = HashMultimap.create();

        final List<ReadonlyBrokerGroupSetting> settings = readonlyBrokerGroupSettingStore.allReadonlyBrokerGroupSettings();
        for (final ReadonlyBrokerGroupSetting setting : settings) {
            map.put(setting.getBrokerGroup(), setting.getSubject());
        }

        readonlyBrokerGroupSettings = map;
    }

    private void refreshPartitionMapping() {
        this.cachedProducerAllocations = partitionService.getLatestProducerAllocations().stream().collect(Collectors.toMap(ProducerAllocation::getSubject, p -> p));
    }

    public ProducerAllocation getProducerAllocation(ClientType clientType, String subject) {
        if (clientType == ClientType.PRODUCER) {
            return cachedProducerAllocations.get(subject);
        } else {
            return null;
        }
    }

    public Set<String> getBrokerGroupReadonlySubjects(final String brokerGroup) {
        return readonlyBrokerGroupSettings.get(brokerGroup);
    }

    private void refreshGroupsAndSubjects() {
        final List<SubjectRoute> subjectRoutes = store.getAllSubjectRoutes();
        if (subjectRoutes == null || subjectRoutes.size() == 0) {
            return;
        }

        Map<String, BrokerGroup> aliveGroups = new HashMap<>(cachedBrokerGroups);

        final Map<String, List<String>> groupSubjects = new HashMap<>();
        final Map<String, List<String>> subjectGroups = new HashMap<>();
        for (SubjectRoute subjectRoute : subjectRoutes) {
            final String subject = subjectRoute.getSubject();
            final List<String> aliveGroupName = new ArrayList<>();

            for (String groupName : subjectRoute.getBrokerGroups()) {
                if (aliveGroups.containsKey(groupName)) {
                    List<String> value = groupSubjects.computeIfAbsent(groupName, k -> new ArrayList<>());
                    value.add(subject);
                    aliveGroupName.add(groupName);
                }
            }
            subjectGroups.put(subject, aliveGroupName);
        }

        cachedSubjectGroups = subjectGroups;
    }

    private void refreshBrokerGroups() {
        final List<BrokerGroup> allBrokerGroups = store.getAllBrokerGroups();
        if (allBrokerGroups == null || allBrokerGroups.size() == 0) {
            return;
        }

        final Map<String, BrokerGroup> normalBrokerGroups = new HashMap<>();
        final Map<String, List<String>> tagToBrokerGroups = new HashMap<>();
        final Map<String, BrokerGroup> delayNewGroups = new HashMap<>();

        for (BrokerGroup brokerGroup : allBrokerGroups) {
            String name = brokerGroup.getGroupName();
            if (Strings.isNullOrEmpty(name)) continue;

            if (brokerGroup.getKind() == BrokerGroupKind.DELAY) {
                delayNewGroups.put(name, brokerGroup);
            } else {
                normalBrokerGroups.put(name, brokerGroup);

                String tag = brokerGroup.getTag();
                tag = Strings.isNullOrEmpty(tag) ? DEFAULT_BROKER_GROUP_TAG : tag;
                List<String> groups = tagToBrokerGroups.computeIfAbsent(tag, key -> new ArrayList<>());
                groups.add(name);
            }

        }

        // ensure default tag has at least one empty list value
        tagToBrokerGroups.computeIfAbsent(DEFAULT_BROKER_GROUP_TAG, key -> new ArrayList<>());

        cachedBrokerGroups = normalBrokerGroups;
        cachedTagToBrokerGroups = tagToBrokerGroups;
        cachedDelayNewGroups = delayNewGroups;
    }

    private void refreshSubjectInfoCache() {
        final List<SubjectInfo> subjectInfoList = store.getAllSubjectInfo();
        if (subjectInfoList == null || subjectInfoList.size() == 0) {
            return;
        }
        final Map<String, SubjectInfo> subjectInfoMap = new HashMap<>();
        for (SubjectInfo subjectInfo : subjectInfoList) {
            String name = subjectInfo.getName();
            if (Strings.isNullOrEmpty(name)) {
                continue;
            }

            subjectInfoMap.put(name, subjectInfo);
        }
        cachedSubjectInfoMap = subjectInfoMap;
    }

    @Override
    public void destroy() {
        SCHEDULE_POOL.shutdown();
    }

    private class RefreshTask implements Runnable {

        @Override
        public void run() {
            try {
                refresh();
            } catch (Exception e) {
                LOG.error("refresh meta info failed", e);
            }
        }
    }
}
