package qunar.tc.qmq.meta.route.impl;

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author zhenwei.liu
 * @since 2019-09-11
 */
public class DefaultSubjectRouter implements SubjectRouter {

    private static final int MIN_SUBJECT_ROUTE_VERSION = 1;
    private static final int MAX_UPDATE_RETRY_TIMES = 5;
    private static final int MIN_BROKER_GROUP_NUM = 2;

    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final Store store;
    private int minBrokerGroupNum = MIN_BROKER_GROUP_NUM;

    public DefaultSubjectRouter(DynamicConfig config, CachedMetaInfoManager cachedMetaInfoManager, Store store) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.store = store;
        config.addListener(conf -> minBrokerGroupNum = conf.getInt("min.group.num", MIN_BROKER_GROUP_NUM));
    }

    @Override
    public List<BrokerGroup> route(String subject, int clientType) {

        if (clientType == ClientType.DELAY_PRODUCER.getCode()) {
            return cachedMetaInfoManager.getDelayNewGroups();
        }

        SubjectInfo subjectInfo = getOrCreateSubjectInfo(subject);

        //query assigned brokers
        final List<String> assignedBrokers = cachedMetaInfoManager.getGroups(subject);

        List<String> newAssignedBrokers;
        if (assignedBrokers == null || assignedBrokers.size() == 0) {
            newAssignedBrokers = assignNewBrokers(subjectInfo, clientType);
        } else {
            newAssignedBrokers = reAssignBrokers(subjectInfo, assignedBrokers, clientType);
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
        final List<String> loadBalanceSelect = selectRandomBrokers(brokerGroupNames, minBrokerGroupNum);
        final int affected = store.insertSubjectRoute(subject, MIN_SUBJECT_ROUTE_VERSION, loadBalanceSelect);
        if (affected == 1) {
            return loadBalanceSelect;
        }

        return findOrUpdateInStore(subjectInfo);
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


    private List<String> reAssignBrokers(SubjectInfo subjectInfo, List<String> assignedBrokers, int clientTypeCode) {
        if (clientTypeCode == ClientType.CONSUMER.getCode()) {
            return assignedBrokers;
        }

        if (assignedBrokers.size() >= minBrokerGroupNum) {
            return assignedBrokers;
        }

        return findOrUpdateInStore(subjectInfo);
    }

    protected <T> List<T> selectRandomBrokers(final List<T> groups, int minSize) {
        if (groups == null || groups.size() == 0) {
            return Collections.emptyList();
        }
        if (groups.size() <= minSize) {
            return groups;
        }

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final Set<T> resultSet = new HashSet<>(minSize);
        while (resultSet.size() <= minSize) {
            final int randomIndex = random.nextInt(groups.size());
            resultSet.add(groups.get(randomIndex));
        }

        return new ArrayList<>(resultSet);
    }

    private List<String> findOrUpdateInStore(final SubjectInfo subjectInfo) {
        String subject = subjectInfo.getName();

        int tries = 0;

        while (tries++ < MAX_UPDATE_RETRY_TIMES) {
            final SubjectRoute subjectRoute = loadSubjectRoute(subject);
            List<String> assignedBrokers = subjectRoute.getBrokerGroups();
            if (assignedBrokers == null) assignedBrokers = new ArrayList<>();

            if (assignedBrokers.size() >= minBrokerGroupNum) return assignedBrokers;

            final List<String> brokerGroupNames = findAvailableBrokerGroupNames(subjectInfo.getTag());
            final List<String> idleBrokers = removeAssignedBrokers(brokerGroupNames, assignedBrokers);
            if (idleBrokers.isEmpty()) return assignedBrokers;

            final List<String> newAssigned = selectRandomBrokers(idleBrokers, minBrokerGroupNum - assignedBrokers.size());
            final List<String> merge = merge(assignedBrokers, newAssigned);
            final int affected = store.updateSubjectRoute(subject, subjectRoute.getVersion(), merge);
            if (affected == 1) {
                return merge;
            }
        }
        throw new RuntimeException("find same room subject route error");
    }

    private List<String> merge(List<String> oldBrokerGroupNames, List<String> select) {
        final Set<String> merge = new HashSet<>();
        merge.addAll(oldBrokerGroupNames);
        merge.addAll(select);
        return new ArrayList<>(merge);
    }

    private List<String> removeAssignedBrokers(List<String> brokerGroupNames, List<String> assignedBrokers) {
        List<String> result = new ArrayList<>();
        for (String name : brokerGroupNames) {
            if (assignedBrokers.contains(name)) continue;

            result.add(name);
        }
        return result;
    }


    private SubjectRoute loadSubjectRoute(String subject) {
        return store.selectSubjectRoute(subject);
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
