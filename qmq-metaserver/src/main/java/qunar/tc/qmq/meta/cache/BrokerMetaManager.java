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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.meta.model.BrokerMeta;
import qunar.tc.qmq.meta.store.BrokerStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-02-26 15:11
 */
public final class BrokerMetaManager implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerMetaManager.class);

    private static final long DEFAULT_REFRESH_PERIOD_SECONDS = 5L;
    private static final int DEFAULT_PORT = 8080;
    private static final String SLASH = "/";
    private static final String EMPTY = "";

    private static final BrokerMetaManager INSTANCE = new BrokerMetaManager();

    private volatile Map<String, BrokerMeta> groupNameToSlaveMap = Maps.newHashMap();

    private ScheduledExecutorService scheduledExecutorService;
    private BrokerStore brokerStore;
    private DynamicConfig httpPortMapConfig;

    private BrokerMetaManager() {
    }

    public static BrokerMetaManager getInstance() {
        return INSTANCE;
    }

    public void init(BrokerStore brokerStore) {
        this.brokerStore = brokerStore;
        this.httpPortMapConfig = DynamicConfigLoader.load("broker-http-port-map.properties", false);
        refresh();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("broker-meta-refresh-%d").build());
        scheduledExecutorService.scheduleAtFixedRate(this::refresh, DEFAULT_REFRESH_PERIOD_SECONDS, DEFAULT_REFRESH_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    private void refresh() {
        List<BrokerMeta> brokers = brokerStore.allBrokers();
        final Map<String, BrokerMeta> slaveMap = new HashMap<>();
        brokers.forEach(brokerMeta -> {
            BrokerRole role = brokerMeta.getRole();
            if (role == BrokerRole.SLAVE || role == BrokerRole.DELAY_SLAVE) {
                slaveMap.put(brokerMeta.getGroup(), brokerMeta);
            }
        });
        if (!slaveMap.isEmpty()) groupNameToSlaveMap = slaveMap;
    }

    public String getSlaveHttpAddress(String groupName) {
        final BrokerMeta meta = groupNameToSlaveMap.get(groupName);
        if (meta == null) return EMPTY;
        String httpPort = slaveHttpPort(meta.getHostname(), meta.getServePort());
        if (Strings.isNullOrEmpty(httpPort)) return EMPTY;
        return meta.getIp() + ":" + httpPort;
    }

    private String slaveHttpPort(String host, int serverPort) {
        try {
            return String.valueOf(httpPortMapConfig.getInt(host + SLASH + serverPort, DEFAULT_PORT));
        } catch (Exception e) {
            LOG.error("Failed to get slave http port.", e);
            return EMPTY;
        }
    }

    @Override
    public void destroy() {
        if (scheduledExecutorService != null)
            scheduledExecutorService.shutdown();
    }
}
