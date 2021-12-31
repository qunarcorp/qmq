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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author yunfeng.yang
 * @since 2018/1/2
 */
public class AliveClientManager {
    private static final Logger LOG = LoggerFactory.getLogger(AliveClientManager.class);

    private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("client-info-cleaner-"));

    private static final long EXPIRE_TIME_MS = TimeUnit.SECONDS.toMillis(200L);
    private static final long CLEAN_PERIOD_HOUR = 1L;

    private static final AliveClientManager INSTANCE = new AliveClientManager();

    private final Map<String, Map<ClientMetaInfo, Long>> allClients;

    private final Map<String, Map<ClientMetaInfo, Long>> allSubject;
    /**
     * k:subject value:<appCode,v>
     */
    private final Map<String, Map<String, List<ClientMetaInfo>>> roomSubject;

    public AliveClientManager() {
        allClients = new ConcurrentHashMap<>();
        allSubject = new ConcurrentHashMap<>();
        roomSubject = new ConcurrentHashMap<>();
        startCleaner();
    }

    public static AliveClientManager getInstance() {
        return INSTANCE;
    }

    public void renew(final MetaInfoRequest request) {
        try {
            QMon.clientRefreshMetaInfoCountInc(request.getSubject());

            final ClientMetaInfo meta = createClientMeta(request);
            final Map<ClientMetaInfo, Long> subjectClients = allClients.computeIfAbsent(request.getSubject(), key -> new ConcurrentHashMap<>());
            subjectClients.put(meta, System.currentTimeMillis());
            final Map<ClientMetaInfo, Long> appCodeClients = allSubject.computeIfAbsent(request.getAppCode(), key -> new ConcurrentHashMap<>());
            appCodeClients.put(meta, System.currentTimeMillis());
        } catch (Exception e) {
            LOG.error("refresh client info error", e);
        }
    }

    private ClientMetaInfo createClientMeta(MetaInfoRequest request) {
        final ClientMetaInfo meta = new ClientMetaInfo();
        meta.setSubject(request.getSubject());
        meta.setConsumerGroup(request.getConsumerGroup());
        meta.setClientTypeCode(request.getClientTypeCode());
        meta.setAppCode(request.getAppCode());
        meta.setClientId(request.getClientId());
        meta.setRoom(request.getClientLdc());
        return meta;
    }

    private void startCleaner() {
        EXECUTOR.scheduleAtFixedRate(new CleanTask(), CLEAN_PERIOD_HOUR, CLEAN_PERIOD_HOUR, TimeUnit.HOURS);
    }


    public Map<String, Map<String, List<ClientMetaInfo>>> getRoomSubject() {
        return roomSubject;
    }

    private class CleanTask implements Runnable {
        @Override
        public void run() {
            try {
                removeExpiredClients();
                loadRoomSubject();
            } catch (Exception e) {
                LOG.error("clean dead client info failed.", e);
            }
        }

        private void loadRoomSubject() {
            for (Map.Entry<String, Map<ClientMetaInfo, Long>> entry : allSubject.entrySet()) {
                Map<String, List<ClientMetaInfo>> appMap = entry.getValue().keySet().stream()
                        .filter(s -> ClientType.isConsumer(s.getClientTypeCode()))
                        .collect(Collectors.groupingBy(ClientMetaInfo::getAppCode));
                for (Map.Entry<String, List<ClientMetaInfo>> appEntry : appMap.entrySet()) {
                    for (Map.Entry<String, List<ClientMetaInfo>> subjectEntry : appEntry.getValue().stream().collect(Collectors.groupingBy(ClientMetaInfo::getSubject)).entrySet()) {
                        int roomSize = subjectEntry.getValue().stream().map(ClientMetaInfo::getRoom).collect(Collectors.toSet()).size();
                        if (roomSize > 1) {
                            Map<String,List<ClientMetaInfo>> appSubjectMap = roomSubject.computeIfAbsent(subjectEntry.getKey(), s -> Maps.newConcurrentMap());
                            appSubjectMap.put(appEntry.getKey(),subjectEntry.getValue());
                        }
                    }
                }
            }
        }
        private void removeExpiredClients() {
            final long now = System.currentTimeMillis();
            for (Map<ClientMetaInfo, Long> map : allClients.values()) {
                for (Map.Entry<ClientMetaInfo, Long> entry : map.entrySet()) {
                    if (now - entry.getValue() >= EXPIRE_TIME_MS) {
                        map.remove(entry.getKey());
                    }
                }
            }
            for (Map<ClientMetaInfo, Long> map : allSubject.values()) {
                for (Map.Entry<ClientMetaInfo, Long> entry : map.entrySet()) {
                    if (now - entry.getValue() >= EXPIRE_TIME_MS) {
                        map.remove(entry.getKey());
                    }
                }
            }
        }
    }
}