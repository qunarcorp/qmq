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

package qunar.tc.qmq.meta.route.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.loadbalance.LoadBalance;
import qunar.tc.qmq.meta.loadbalance.RandomLoadBalance;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.*;

/**
 * @author keli.wang
 * @since 2017/12/4
 */
public class DefaultSubjectRouter implements SubjectRouter {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSubjectRouter.class);

    private static final int MIN_SUBJECT_ROUTE_VERSION = 1;
    private static final int DEFAULT_MIN_NUM = 2;
    private static final int MAX_UPDATE_RETRY_TIMES = 5;

    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final Store store;
    private final LoadBalance<String> loadBalance;
    private int minGroupNum = DEFAULT_MIN_NUM;

    public DefaultSubjectRouter(final DynamicConfig config, final CachedMetaInfoManager cachedMetaInfoManager, final Store store) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.store = store;
        this.loadBalance = new RandomLoadBalance<>();

        config.addListener(conf -> minGroupNum = conf.getInt("min.group.num", DEFAULT_MIN_NUM));
    }

    @Override
    public List<BrokerGroup> route(final String subject, final MetaInfoRequest request) {
        try {
            QMon.clientSubjectRouteCountInc(subject);
            return doRoute(subject, request.getClientTypeCode());
        } catch (Throwable e) {
            LOG.error("find subject route error", e);
            return Collections.emptyList();
        }
    }

    private List<BrokerGroup> doRoute(String subject, int clientTypeCode) {
        SubjectInfo subjectInfo = getOrCreateSubjectInfo(subject);

        //query assigned brokers
        final List<String> assignedBrokers = cachedMetaInfoManager.getGroups(subject);

        List<String> newAssignedBrokers;
        if (assignedBrokers == null || assignedBrokers.size() == 0) {
            newAssignedBrokers = assignNewBrokers(subjectInfo, clientTypeCode);
        } else {
            newAssignedBrokers = reAssignBrokers(subjectInfo, assignedBrokers, clientTypeCode);
        }

        return selectExistedBrokerGroups(newAssignedBrokers);
    }

    private SubjectInfo getOrCreateSubjectInfo(String subject) {
        SubjectInfo subjectInfo = cachedMetaInfoManager.getSubjectInfo(subject);
        if (subjectInfo == null) {
            // just add monitor event, will use broker_groups with default tag
            QMon.subjectInfoNotFound(subject);
            subjectInfo = new SubjectInfo();
            subjectInfo.setName(subject);
        }
        return subjectInfo;
    }

    private List<String> assignNewBrokers(SubjectInfo subjectInfo, int clientTypeCode) {
        if (clientTypeCode == ClientType.CONSUMER.getCode()) {
            return Collections.emptyList();
        }

        String subject = subjectInfo.getName();
        final List<String> brokerGroupNames = findAvailableBrokerGroupNames(subjectInfo.getTag());
        final List<String> loadBalanceSelect = loadBalance.select(subject, brokerGroupNames, minGroupNum);
        final int affected = store.insertSubjectRoute(subject, MIN_SUBJECT_ROUTE_VERSION, loadBalanceSelect);
        if (affected == 1) {
            return loadBalanceSelect;
        }

        return findOrUpdateInStore(subjectInfo);
    }

    private List<String> reAssignBrokers(SubjectInfo subjectInfo, List<String> assignedBrokers, int clientTypeCode) {
        if (clientTypeCode == ClientType.CONSUMER.getCode()) {
            return assignedBrokers;
        }

        if (assignedBrokers.size() >= minGroupNum) {
            return assignedBrokers;
        }

        return findOrUpdateInStore(subjectInfo);
    }

    private List<String> findOrUpdateInStore(final SubjectInfo subjectInfo) {
        String subject = subjectInfo.getName();

        int tries = 0;

        while (tries++ < MAX_UPDATE_RETRY_TIMES) {
            final SubjectRoute subjectRoute = loadSubjectRoute(subject);
            List<String> assignedBrokers = subjectRoute.getBrokerGroups();
            if (assignedBrokers == null) assignedBrokers = new ArrayList<>();

            if (assignedBrokers.size() >= minGroupNum) return assignedBrokers;

            final List<String> brokerGroupNames = findAvailableBrokerGroupNames(subjectInfo.getTag());
            final List<String> idleBrokers = removeAssignedBrokers(brokerGroupNames, assignedBrokers);
            if (idleBrokers.isEmpty()) return assignedBrokers;

            final List<String> newAssigned = loadBalance.select(subject, idleBrokers, minGroupNum - assignedBrokers.size());
            final List<String> merge = merge(assignedBrokers, newAssigned);
            final int affected = store.updateSubjectRoute(subject, subjectRoute.getVersion(), merge);
            if (affected == 1) {
                return merge;
            }
        }
        throw new RuntimeException("find same room subject route error");
    }

    private List<String> removeAssignedBrokers(List<String> brokerGroupNames, List<String> assignedBrokers) {
        List<String> result = new ArrayList<>();
        for (String name : brokerGroupNames) {
            if (assignedBrokers.contains(name)) continue;

            result.add(name);
        }
        return result;
    }

    private List<String> merge(List<String> oldBrokerGroupNames, List<String> select) {
        final Set<String> merge = new HashSet<>();
        merge.addAll(oldBrokerGroupNames);
        merge.addAll(select);
        return new ArrayList<>(merge);
    }

    private SubjectRoute loadSubjectRoute(String subject) {
        return store.selectSubjectRoute(subject);
    }

    private List<String> findAvailableBrokerGroupNames(String tag) {
        List<String> brokerGroupNames = cachedMetaInfoManager.getAllBrokerGroupNamesByTag(tag);
        if (brokerGroupNames == null || brokerGroupNames.isEmpty()) {
            brokerGroupNames = cachedMetaInfoManager.getAllDefaultTagBrokerGroupNames();
        }

        if (brokerGroupNames == null || brokerGroupNames.isEmpty()) {
            throw new RuntimeException("no broker groups");
        }

        List<String> result = new ArrayList<>();
        for (String name : brokerGroupNames) {
            BrokerGroup brokerGroup = cachedMetaInfoManager.getBrokerGroup(name);
            if (brokerGroup == null || brokerGroup.getBrokerState() == BrokerState.NRW) continue;
            result.add(name);
        }
        return result;
    }

    private List<BrokerGroup> selectExistedBrokerGroups(final List<String> brokerGroupNames) {
        if (brokerGroupNames == null || brokerGroupNames.isEmpty()) {
            return Collections.emptyList();
        }
        final List<BrokerGroup> result = new ArrayList<>();
        for (String name : brokerGroupNames) {
            final BrokerGroup brokerGroup = cachedMetaInfoManager.getBrokerGroup(name);
            if (brokerGroup != null) {
                result.add(brokerGroup);
            }
        }
        return result;
    }
}
