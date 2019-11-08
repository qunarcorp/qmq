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

package qunar.tc.qmq.meta.store.impl;

import com.google.common.base.Strings;
import java.util.Date;
import java.util.List;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;

/**
 * @author keli.wang
 * @since 2017/12/5
 */
public class ClientMetaInfoStoreImpl implements ClientMetaInfoStore {

    private static final String QUERY_CONSUMER_SQL = "SELECT subject_info, consume_strategy, online_status, client_type, consumer_group, client_id, app_code, room FROM client_meta_info WHERE subject_info=? AND client_type=?";
    private static final String QUERY_CONSUMER_BY_SUBJECT_CLIENT_SQL = "SELECT subject_info, consume_strategy, online_status, client_type, consumer_group, client_id, app_code, room FROM client_meta_info WHERE subject_info=? AND consumer_group = ? AND client_id = ? AND client_type=?";
    private static final String QUERY_CLIENT_AFTER_DATE_SQL =
            "SELECT subject_info, consume_strategy, online_status, client_type, consumer_group, client_id, app_code, room "
                    + "from client_meta_info "
                    + "where client_type=? and online_status = ? and consume_strategy = ? and update_time > ?";
    private static final String QUERY_CLIENT_AFTER_DATE_BY_SUBJECT_AND_CGROUP_SQL =
            "SELECT subject_info, consume_strategy, online_status, client_type, consumer_group, client_id, app_code, room "
                    + "from client_meta_info "
                    + "where subject_info = ? and consumer_group = ? and client_type=? and online_status = ? and consume_strategy = ? and update_time > ?";
    private static final String UPDATE_CLIENT_STATE_SQL = "update client_meta_info set update_time = now(), online_status = ?, consume_strategy = ? where subject_info = ? and client_type = ? and consumer_group = ? and client_id = ?";

    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();

    private static final RowMapper<ClientMetaInfo> clientMetaInfoRowMapper = (rs, rowNum) -> {
        String subject = rs.getString("subject_info");
        int clientType = rs.getInt("client_type");
        String consumerGroup = rs.getString("consumer_group");
        String consumeStrategyName = rs.getString("consume_strategy");
        ConsumeStrategy consumeStrategy = null;
        if (!Strings.isNullOrEmpty(consumeStrategyName)) {
            consumeStrategy = ConsumeStrategy.valueOf(consumeStrategyName);
        }
        String clientId = rs.getString("client_id");
        String appCode = rs.getString("app_code");
        String room = rs.getString("room");
        OnOfflineState onOfflineState = OnOfflineState.valueOf(rs.getString("online_status"));
        return new ClientMetaInfo(
                subject,
                clientType,
                appCode,
                room,
                clientId,
                consumerGroup,
                onOfflineState,
                consumeStrategy
        );
    };

    @Override
    public List<ClientMetaInfo> queryConsumer(String subject) {
        return jdbcTemplate.query(QUERY_CONSUMER_SQL, clientMetaInfoRowMapper, subject, ClientType.CONSUMER.getCode());
    }

    @Override
    public ClientMetaInfo queryConsumer(String subject, String consumerGroup, String clientId) {
        return DataAccessUtils.singleResult(
                jdbcTemplate
                        .query(QUERY_CONSUMER_BY_SUBJECT_CLIENT_SQL, clientMetaInfoRowMapper, subject, consumerGroup,
                                clientId, ClientType.CONSUMER.getCode())
        );
    }

    @Override
    public List<ClientMetaInfo> queryClientsUpdateAfterDate(ClientType clientType, OnOfflineState onlineStatus,
            ConsumeStrategy consumeStrategy, Date updateDate) {
        return jdbcTemplate
                .query(QUERY_CLIENT_AFTER_DATE_SQL, clientMetaInfoRowMapper, clientType.getCode(), onlineStatus.name(),
                        consumeStrategy.name(), updateDate);

    }

    @Override
    public List<ClientMetaInfo> queryClientsUpdateAfterDate(String subject, String consumerGroup, ClientType clientType,
            OnOfflineState onlineStatus, ConsumeStrategy consumeStrategy, Date updateDate) {
        return jdbcTemplate.query(QUERY_CLIENT_AFTER_DATE_BY_SUBJECT_AND_CGROUP_SQL, clientMetaInfoRowMapper, subject,
                consumerGroup, clientType.getCode(), onlineStatus.name(), consumeStrategy.name(), updateDate);
    }

    @Override
    public int updateClientOnlineState(ClientMetaInfo clientMetaInfo, ConsumeStrategy consumeStrategy) {
        return jdbcTemplate.update(UPDATE_CLIENT_STATE_SQL,
                clientMetaInfo.getOnlineStatus().name(),
                consumeStrategy.name(),
                clientMetaInfo.getSubject(),
                clientMetaInfo.getClientTypeCode(),
                defaultEmpty(clientMetaInfo.getConsumerGroup()),
                clientMetaInfo.getClientId()
        );
    }

    private String defaultEmpty(String s) {
        return s == null ? "" : s;
    }
}
