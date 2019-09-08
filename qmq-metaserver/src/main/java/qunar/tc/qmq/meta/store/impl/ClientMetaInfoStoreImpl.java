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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;

import java.util.Date;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/12/5
 */
public class ClientMetaInfoStoreImpl implements ClientMetaInfoStore {

    private static final String QUERY_CONSUMER_SQL = "SELECT subject_info,client_type,consumer_group,client_id,app_code,room FROM client_meta_info WHERE subject_info=? AND client_type=?";
    private static final String QUERY_CLIENT_AFTER_DATE_SQL = "SELECT subject_info,client_type,consumer_group,client_id,app_code,room from client_meta_info where client_type=? and online_status = ? and update_time > ?";
    private static final String QUERY_CLIENT_AFTER_DATE_SQL2 = "SELECT subject_info,client_type,consumer_group,client_id,app_code,room from client_meta_info where subject = ?, consumer_group = ?, client_type=? and online_status = ? and update_time > ?";
    private static final String UPDATE_CLIENT_STATE_SQL = "update client_meta_info set update_time = now(), online_status = ? where subject_info = ? and client_type = ? and consumer_group = ? and client_id = ?";

    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();

    private static final RowMapper<ClientMetaInfo> clientMetaInfoRowMapper = (rs, rowNum) -> {
        final ClientMetaInfo meta = new ClientMetaInfo();
        meta.setSubject(rs.getString("subject_info"));
        meta.setClientTypeCode(rs.getInt("client_type"));
        meta.setConsumerGroup(rs.getString("consumer_group"));
        meta.setClientId(rs.getString("client_id"));
        meta.setAppCode(rs.getString("app_code"));
        meta.setRoom(rs.getString("room"));
        meta.setOnlineStatus(OnOfflineState.valueOf(rs.getString("online_status")));
        return meta;
    };

    @Override
    public List<ClientMetaInfo> queryConsumer(String subject) {
        return jdbcTemplate.query(QUERY_CONSUMER_SQL, clientMetaInfoRowMapper, subject, ClientType.CONSUMER.getCode());
    }

    @Override
    public List<ClientMetaInfo> queryClientsUpdateAfterDate(ClientType clientType, OnOfflineState onlineStatus, Date updateDate) {
        return jdbcTemplate.query(QUERY_CLIENT_AFTER_DATE_SQL, clientMetaInfoRowMapper, clientType.getCode(), onlineStatus.name(), updateDate);
    }

    @Override
    public List<ClientMetaInfo> queryClientsUpdateAfterDate(String subject, String consumerGroup, ClientType clientType, OnOfflineState onlineStatus, Date updateDate) {
        return jdbcTemplate.query(QUERY_CLIENT_AFTER_DATE_SQL, clientMetaInfoRowMapper, subject, consumerGroup, clientType.getCode(), onlineStatus.name(), updateDate);
    }

    @Override
    public int updateClientOnlineState(ClientMetaInfo clientMetaInfo) {
        return jdbcTemplate.update(UPDATE_CLIENT_STATE_SQL,
                clientMetaInfo.getOnlineStatus().name(),
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
