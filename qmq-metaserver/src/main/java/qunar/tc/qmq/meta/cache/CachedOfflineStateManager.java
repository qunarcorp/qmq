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

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.meta.model.ClientOfflineState;
import qunar.tc.qmq.meta.store.ClientOfflineStore;
import qunar.tc.qmq.meta.store.impl.ClientOfflineStoreImpl;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * yiqun.fan@qunar.com 2018/3/2
 */
public class CachedOfflineStateManager implements Disposable {
    private static final Logger log = LoggerFactory.getLogger(CachedOfflineStateManager.class);
    private static final long REFRESH_PERIOD_SECONDS = 30L;
    private static final String CLIENT_STATE_KEY_SEPARATOR = "$";
    private static final String CLIENTID_OF_GROUP = "*";
    private static final QmqCounter REFRESH_ERROR = Metrics.counter("refresh_onofflinestate_error");

    public static final Supplier<CachedOfflineStateManager> SUPPLIER = Suppliers.memoize(CachedOfflineStateManager::new)::get;

    private final ClientOfflineStore store = new ClientOfflineStoreImpl();
    private final ScheduledExecutorService scheduledExecutor;

    private volatile long updateTime = -1;
    private volatile Map<String, OnOfflineState> groupStateMap = new HashMap<>();
    private volatile Map<String, OnOfflineState> clientStateMap = new HashMap<>();

    private static String clientStateKey(String clientId, String subject, String consumerGroup) {
        return clientId + CLIENT_STATE_KEY_SEPARATOR + subject + CLIENT_STATE_KEY_SEPARATOR + consumerGroup;
    }

    private CachedOfflineStateManager() {
        refresh();
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("meta-info-offlinestate-refresh-%d").build());
        scheduledExecutor.scheduleAtFixedRate(this::refresh, REFRESH_PERIOD_SECONDS, REFRESH_PERIOD_SECONDS, TimeUnit.SECONDS);
        log.info("CachedOfflineStateManager started");
    }

    private void refresh() {
        try {
            long updateTime = store.now();
            List<ClientOfflineState> states = store.selectAll();
            Map<String, OnOfflineState> groupStateMap = new HashMap<>();
            Map<String, OnOfflineState> clientStateMap = new HashMap<>();
            for (ClientOfflineState state : states) {
                if (state == null) continue;

                String key = clientStateKey(state.getClientId(), state.getSubject(), state.getConsumerGroup());
                if (CLIENTID_OF_GROUP.equals(state.getClientId())) {
                    groupStateMap.put(key, state.getState());
                } else {
                    clientStateMap.put(key, state.getState());
                }
            }
            this.updateTime = updateTime;
            this.groupStateMap = groupStateMap;
            this.clientStateMap = clientStateMap;
            log.info("refreshed onoffline state {}", updateTime);
        } catch (Exception e) {
            log.error("refresh OfflineState exception", e);
            REFRESH_ERROR.inc();
        }
    }

    public long getLastUpdateTimestamp() {
        return updateTime;
    }

    public OnOfflineState queryClientState(String clientId, String subject, String consumerGroup) {
        OnOfflineState state = groupStateMap.get(clientStateKey(CLIENTID_OF_GROUP, subject, consumerGroup));
        if (state != null) {
            return state;
        }
        state = clientStateMap.get(clientStateKey(clientId, subject, consumerGroup));
        return state == null ? OnOfflineState.ONLINE : state;
    }

    public void insertOrUpdate(ClientOfflineState clientState) {
        if (clientState.getState() == OnOfflineState.OFFLINE) {
            store.insertOrUpdate(clientState);
        } else {
            if (CLIENTID_OF_GROUP.equals(clientState.getClientId())) {
                store.delete(clientState.getSubject(), clientState.getConsumerGroup());
            } else {
                store.delete(clientState.getClientId(), clientState.getSubject(), clientState.getConsumerGroup());
            }
        }
    }

    public void queryClientState(ClientOfflineState clientState) {
        Optional<ClientOfflineState> result = store.select(clientState.getClientId(), clientState.getSubject(), clientState.getConsumerGroup());
        if (result.isPresent()) {
            clientState.setState(result.get().getState());
        } else {
            clientState.setState(OnOfflineState.ONLINE);
        }
    }

    @Override
    public void destroy() {
        scheduledExecutor.shutdown();
        log.info("CachedOfflineStateManager destoried");
    }
}
