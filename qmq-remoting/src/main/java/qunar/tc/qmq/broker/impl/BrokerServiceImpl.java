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

package qunar.tc.qmq.broker.impl;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metainfoclient.MetaInfo;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.protocol.MetaInfoResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerServiceImpl implements BrokerService, MetaInfoClient.ResponseSubscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServiceImpl.class);

    private final ConcurrentMap<String, BrokerClusterInfo> clusterMap = new ConcurrentHashMap<>();

    private final DefaultMetaInfoService metaInfoService;
    private String appCode;

    public BrokerServiceImpl(DefaultMetaInfoService metaInfoService) {
        this.metaInfoService = metaInfoService;
        this.metaInfoService.registerResponseSubscriber(this);
    }

    private void updateOnDemand(String key, BrokerClusterInfo clusterInfo) {
        BrokerClusterInfo oldClusterInfo = clusterMap.putIfAbsent(key, clusterInfo);
        if (oldClusterInfo == null) {
            return;
        }

        if (isEquals(oldClusterInfo, clusterInfo)) {
            return;
        }

        List<BrokerGroupInfo> groups = clusterInfo.getGroups();
        List<BrokerGroupInfo> updated = new ArrayList<>(groups.size());
        for (BrokerGroupInfo group : groups) {
            BrokerGroupInfo oldGroup = oldClusterInfo.getGroupByName(group.getGroupName());
            if (oldGroup == null) {
                updated.add(group);
                continue;
            }

            if (oldGroup.getMaster().equals(group.getMaster())) {
                BrokerGroupInfo copy = new BrokerGroupInfo(group.getGroupIndex(),
                        group.getGroupName(), group.getMaster(), group.getSlaves(), oldGroup.getCircuitBreaker());
                updated.add(copy);
                continue;
            }

            updated.add(group);
        }

        BrokerClusterInfo updatedCluster = new BrokerClusterInfo(updated);
        clusterMap.put(key, updatedCluster);
    }

    private boolean isEquals(BrokerClusterInfo oldClusterInfo, BrokerClusterInfo clusterInfo) {
        List<BrokerGroupInfo> groups = clusterInfo.getGroups();
        if (groups.size() != oldClusterInfo.getGroups().size()) return false;

        for (BrokerGroupInfo group : groups) {
            BrokerGroupInfo oldGroup = oldClusterInfo.getGroupByName(group.getGroupName());
            if (oldGroup == null) return false;

            if (!oldGroup.getMaster().equals(group.getMaster())) return false;
        }
        return true;
    }

    private void logMetaInfo(MetaInfo metaInfo, BrokerClusterInfo cluster) {
        if (cluster == null) {
            LOGGER.error("meta server return empty broker, will retry in a few seconds. subject={}, client={}", metaInfo.getSubject(), metaInfo.getClientType());
        } else {
            LOGGER.info("meta server return empty broker, will retry in a few seconds. subject={}, client={}", metaInfo.getSubject(), metaInfo.getClientType());
        }
    }

    private boolean isEmptyCluster(MetaInfo metaInfo) {
        return metaInfo.getClientType() != ClientType.CONSUMER
                && metaInfo.getClusterInfo().getGroups().isEmpty();
    }

    @Override
    public BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject) {
        return getClusterBySubject(clientType, subject, "");
    }

    @Override
    public BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject, String group) {
        // 这个key上加group不兼容MetaInfoResponse
        String key = MapKeyBuilder.buildMetaInfoKey(clientType, subject);
        return clusterMap.get(key);
    }

    @Override
    public void refresh(ClientType clientType, String subject) {
        refresh(clientType, subject, "");
    }

    @Override
    public void refresh(ClientType clientType, String subject, String group) {
        metaInfoService.request(DefaultMetaInfoService.buildRequestParam(clientType, subject, group, appCode), ClientRequestType.HEARTBEAT);
    }

    @Override
    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        MetaInfo metaInfo = parseResponse(response);
        if (metaInfo != null) {
            LOGGER.debug("meta info: {}", metaInfo);
            String key = MapKeyBuilder.buildMetaInfoKey(metaInfo.getClientType(), metaInfo.getSubject());
            BrokerClusterInfo brokerClusterInfo = clusterMap.get(key);
            if (isEmptyCluster(metaInfo)) {
                logMetaInfo(metaInfo, brokerClusterInfo);
                return;
            }
            updateOnDemand(key, metaInfo.getClusterInfo());
        } else {
            LOGGER.warn("request meta info fail, will retry in a few seconds.");
        }
    }

    private MetaInfo parseResponse(MetaInfoResponse response) {
        if (response == null) return null;

        String subject = response.getSubject();
        if (Strings.isNullOrEmpty(subject)) return null;

        ClientType clientType = parseClientType(response);
        if (clientType == null) return null;

        BrokerCluster cluster = response.getBrokerCluster();
        List<BrokerGroup> groups = cluster == null ? null : cluster.getBrokerGroups();
        if (groups == null || groups.isEmpty()) {
            return new MetaInfo(subject, clientType, new BrokerClusterInfo());
        }

        List<BrokerGroup> validBrokers = new ArrayList<>(groups.size());
        for (BrokerGroup group : groups) {
            if (inValid(group)) continue;

            BrokerState state = group.getBrokerState();
            if (clientType.isConsumer() && state.canRead()) {
                validBrokers.add(group);
            } else if (clientType.isProducer() && state.canWrite()) {
                validBrokers.add(group);
            }
        }
        if (validBrokers.isEmpty()) {
            return new MetaInfo(subject, clientType, new BrokerClusterInfo());
        }
        List<BrokerGroupInfo> groupInfos = new ArrayList<>(validBrokers.size());
        for (int i = 0; i < validBrokers.size(); i++) {
            BrokerGroup bg = validBrokers.get(i);
            groupInfos.add(new BrokerGroupInfo(i, bg.getGroupName(), bg.getMaster(), bg.getSlaves()));
        }
        BrokerClusterInfo clusterInfo = new BrokerClusterInfo(groupInfos);
        return new MetaInfo(subject, clientType, clusterInfo);
    }

    private boolean inValid(BrokerGroup group) {
        return group == null || Strings.isNullOrEmpty(group.getGroupName()) || Strings.isNullOrEmpty(group.getMaster());
    }

    private ClientType parseClientType(MetaInfoResponse response) {
        return ClientType.of(response.getClientTypeCode());
    }
}
