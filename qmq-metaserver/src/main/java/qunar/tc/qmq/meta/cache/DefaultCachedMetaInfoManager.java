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
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerGroupKind;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionSet;
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
public class DefaultCachedMetaInfoManager implements Disposable, CachedMetaInfoManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCachedMetaInfoManager.class);

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
     * subject -> partitionSet
     */
    private volatile Map<String, PartitionSet> latestPartitionSets = new HashMap<>();

    private volatile Map<String, List<PartitionSet>> allPartitionSets = new ConcurrentHashMap<>();

    /**
     * subject+id -> partition
     */
    private volatile Map<String, Partition> subjectPid2Partition = new HashMap<>();

    /**
     * subject+consumerGroup -> partitionAllocation
     */
    private volatile Map<String, PartitionAllocation> subjectConsumerGroup2partitionAllocation = new HashMap<>();

    /**
     * brokerGroupName -> Subject List
     */
    private volatile SetMultimap<String, String> readonlyBrokerGroupSettings = HashMultimap.create();

    public DefaultCachedMetaInfoManager(DynamicConfig config, Store store, ReadonlyBrokerGroupSettingStore readonlyBrokerGroupSettingStore, PartitionService partitionService) {
        this(store, readonlyBrokerGroupSettingStore, partitionService, config.getLong("refresh.period.seconds", DEFAULT_REFRESH_PERIOD_SECONDS));
    }

    public DefaultCachedMetaInfoManager(Store store, ReadonlyBrokerGroupSettingStore readonlyBrokerGroupSettingStore, PartitionService partitionService) {
        this(store, readonlyBrokerGroupSettingStore, partitionService, DEFAULT_REFRESH_PERIOD_SECONDS);
    }

    private DefaultCachedMetaInfoManager(Store store, ReadonlyBrokerGroupSettingStore readonlyBrokerGroupSettingStore, PartitionService partitionService, long refreshPeriodSeconds) {
        this.refreshPeriodSeconds = refreshPeriodSeconds;
        this.store = store;
        this.readonlyBrokerGroupSettingStore = readonlyBrokerGroupSettingStore;
        this.partitionService = partitionService;
        doRefresh();
        initRefreshTask();
    }

    @Override
    public SubjectInfo getSubjectInfo(String subject) {
        return cachedSubjectInfoMap.get(subject);
    }

    private void initRefreshTask() {
        SCHEDULE_POOL.scheduleAtFixedRate(this::doRefresh, refreshPeriodSeconds, refreshPeriodSeconds, TimeUnit.SECONDS);
    }

    @Override
    public List<String> getBrokerGroups(String subject) {
        final List<String> groups = cachedSubjectGroups.get(subject);
        if (groups == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(groups);
    }

    @Override
    public List<String> getAllBrokerGroupNamesByTag(String tag) {
        if (tag == null) {
            return ImmutableList.of();
        }
        List<String> groupNames = cachedTagToBrokerGroups.get(tag);
        return groupNames == null ? ImmutableList.of() : ImmutableList.copyOf(groupNames);
    }

    @Override
    public List<String> getAllDefaultTagBrokerGroupNames() {
        return ImmutableList.copyOf(cachedTagToBrokerGroups.get(DEFAULT_BROKER_GROUP_TAG));
    }

    @Override
    public BrokerGroup getBrokerGroup(String groupName) {
        return cachedBrokerGroups.get(groupName);
    }

    @Override
    public List<BrokerGroup> getDelayNewGroups() {
        return Lists.newArrayList(cachedDelayNewGroups.values());
    }

    @Override
    public PartitionSet getLatestPartitionSet(String subject) {
        return latestPartitionSets.get(createPartitionSetKey(subject));
    }

    @Override
    public List<PartitionSet> getPartitionSets(String subject) {
        return allPartitionSets.get(createPartitionSetKey(subject));
    }

    @Override
    public Partition getPartition(String subject, int partitionId) {
        return subjectPid2Partition.get(createPartitionKey(subject, partitionId));
    }

    @Override
    public PartitionAllocation getPartitionAllocation(String subject, String consumerGroup) {
        return subjectConsumerGroup2partitionAllocation.get(createPartitionAllocationKey(subject, consumerGroup));
    }

    @Override
    public void refresh() {
        SCHEDULE_POOL.execute(this::doRefresh);
    }

    private void doRefresh() {
        LOGGER.info("refresh meta info");
        refreshBrokerGroups();
        refreshSubjectInfoCache();
        refreshGroupsAndSubjects();
        refreshReadonlyBrokerGroupSettings();
        refreshPartitions();
        refreshPartitionAllocations();
    }

    private void refreshReadonlyBrokerGroupSettings() {
        final HashMultimap<String, String> map = HashMultimap.create();

        final List<ReadonlyBrokerGroupSetting> settings = readonlyBrokerGroupSettingStore.allReadonlyBrokerGroupSettings();
        for (final ReadonlyBrokerGroupSetting setting : settings) {
            map.put(setting.getBrokerGroup(), setting.getSubject());
        }

        readonlyBrokerGroupSettings = map;
    }

    private void refreshPartitions() {
        this.subjectPid2Partition = partitionService.getAllPartitions().stream().collect(
                Collectors.toMap(p -> createPartitionKey(p.getSubject(), p.getPartitionId()), p -> p));
        List<PartitionSet> allPartitionSets = partitionService.getAllPartitionSets();
        for (PartitionSet ps : allPartitionSets) {
            String subject = ps.getSubject();
            String key = createPartitionSetKey(subject);
            List<PartitionSet> partitionSets = this.allPartitionSets.computeIfAbsent(key, k -> Lists.newArrayList());
            partitionSets.add(ps);
        }
        this.latestPartitionSets = partitionService.getLatestPartitionSets().stream().collect(
                Collectors.toMap(p -> createPartitionSetKey(p.getSubject()), p -> p));
    }

    private void refreshPartitionAllocations() {
        this.subjectConsumerGroup2partitionAllocation = partitionService.getLatestPartitionAllocations().stream().collect(
                Collectors.toMap(pa -> createPartitionAllocationKey(pa.getSubject(), pa.getConsumerGroup()), pa -> pa));
    }

    private String createPartitionSetKey(String subject) {
        return subject;
    }

    private String createPartitionKey(String subject, int partitionId) {
        return subject + ":" + partitionId;
    }

    private String createPartitionAllocationKey(String subject, String consumerGroup) {
        return subject + ":" + consumerGroup;
    }

    @Override
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
}
