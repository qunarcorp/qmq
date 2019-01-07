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

import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.metainfoclient.MetaInfo;
import qunar.tc.qmq.metainfoclient.MetaInfoService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerServiceImpl implements BrokerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServiceImpl.class);

    private final ConcurrentMap<String, ClusterFuture> clusterMap = new ConcurrentHashMap<>();

    private final MetaInfoService metaInfoService;
    private String appCode;

    public BrokerServiceImpl(MetaInfoService metaInfoService) {
        this.metaInfoService = metaInfoService;
        this.metaInfoService.register(this);
    }

    @Subscribe
    public void onReceiveMetaInfo(MetaInfo metaInfo) {
        String key = MapKeyBuilder.buildMetaInfoKey(metaInfo.getClientType(), metaInfo.getSubject());
        ClusterFuture future = clusterMap.get(key);
        if (isEmptyCluster(metaInfo)) {
            logMetaInfo(metaInfo, future);
            return;
        }
        if (future == null) {
            future = new ClusterFuture(metaInfo.getClusterInfo());
            ClusterFuture oldFuture = clusterMap.putIfAbsent(key, future);
            if (oldFuture != null) {
                oldFuture.set(metaInfo.getClusterInfo());
            }
        } else {
            updateOnDemand(future, metaInfo.getClusterInfo());
        }
    }

    private void updateOnDemand(ClusterFuture future, BrokerClusterInfo clusterInfo) {
        BrokerClusterInfo oldClusterInfo = future.cluster.get();
        if (oldClusterInfo == null) {
            future.set(clusterInfo);
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
        future.set(updatedCluster);
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

    private void logMetaInfo(MetaInfo metaInfo, ClusterFuture future) {
        if (future == null || future.cluster.get() == null) {
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
        ClusterFuture future = clusterMap.get(key);
        MetaInfoService.MetaInfoRequestParam requestParam = MetaInfoService.buildRequestParam(clientType, subject, group, appCode);
        if (future == null) {
            future = request(requestParam, false);
        } else {
            metaInfoService.tryAddRequest(requestParam);
        }
        return future.get();
    }

    @Override
    public void refresh(ClientType clientType, String subject) {
        refresh(clientType, subject, "");
    }

    @Override
    public void refresh(ClientType clientType, String subject, String group) {
        request(MetaInfoService.buildRequestParam(clientType, subject, group, appCode), true);
    }

    @Override
    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    private ClusterFuture request(MetaInfoService.MetaInfoRequestParam requestParam, boolean refresh) {
        String key = MapKeyBuilder.buildMetaInfoKey(requestParam.getClientType(), requestParam.getSubject());
        ClusterFuture newFuture = new ClusterFuture();
        ClusterFuture oldFuture = clusterMap.putIfAbsent(key, newFuture);
        if (oldFuture != null) {
            if (refresh && !oldFuture.inRequest.get()) {
                oldFuture.inRequest.set(true);
                metaInfoService.requestWrapper(requestParam);
            } else {
                metaInfoService.tryAddRequest(requestParam);
            }
            return oldFuture;
        }
        metaInfoService.requestWrapper(requestParam);
        return newFuture;
    }

    private static final class ClusterFuture {
        private final CountDownLatch latch;
        private final AtomicReference<BrokerClusterInfo> cluster;
        private final AtomicBoolean inRequest;

        ClusterFuture() {
            latch = new CountDownLatch(1);
            cluster = new AtomicReference<>(null);
            inRequest = new AtomicBoolean(true);
        }

        ClusterFuture(BrokerClusterInfo cluster) {
            latch = new CountDownLatch(0);
            this.cluster = new AtomicReference<>(cluster);
            inRequest = new AtomicBoolean(false);
        }

        void set(BrokerClusterInfo cluster) {
            this.cluster.set(cluster);
            latch.countDown();
            inRequest.set(false);
        }

        public BrokerClusterInfo get() {
            while (true) {
                try {
                    latch.await();
                    break;
                } catch (Exception e) {
                    LOGGER.warn("get broker cluster info be interrupted, and ignore");
                }
            }
            return cluster.get();
        }
    }
}
