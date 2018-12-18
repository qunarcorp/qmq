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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.meta.model.ClientOfflineState;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.meta.store.JdbcTemplateHolder;
import qunar.tc.qmq.meta.store.ClientOfflineStore;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

/**
 * yiqun.fan@qunar.com 2018/2/28
 */
public class ClientOfflineStoreImpl implements ClientOfflineStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientOfflineStoreImpl.class);

    private static final String SELECT_NOW_SQL = "SELECT now() as ts";
    private static final String SELECT_SQL = "SELECT id,client_id,subject_info,consumer_group,state FROM client_offline_state WHERE client_id=? and subject_info=? and consumer_group=?";
    private static final String SELECT_ALL_SQL = "SELECT id,client_id,subject_info,consumer_group,state FROM client_offline_state";
    private static final String INSERT_SQL = "INSERT INTO client_offline_state(client_id,subject_info,consumer_group,state) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE state=?";
    private static final String DELETE_BY_SUBJECT_GROUP_SQL = "DELETE FROM client_offline_state WHERE subject_info=? and consumer_group=?";
    private static final String DELETE_SQL = "DELETE FROM client_offline_state WHERE client_id=? and subject_info=? and consumer_group=?";

    private static final RowMapper<ClientOfflineState> ROW_MAPPER = (rs, rowNum) -> {
        try {
            ClientOfflineState state = new ClientOfflineState();
            state.setId(rs.getLong("id"));
            state.setClientId(rs.getString("client_id"));
            state.setSubject(rs.getString("subject_info"));
            state.setConsumerGroup(rs.getString("consumer_group"));
            state.setState(OnOfflineState.fromCode(rs.getInt("state")));
            return state;
        } catch (Exception e) {
            LOGGER.error("selectAll exception", e);
            return null;
        }
    };

    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();

    @Override
    public long now() {
        List<Long> results = jdbcTemplate.query(SELECT_NOW_SQL, (rs, rowNum) -> {
            try {
                return rs.getTimestamp("ts").getTime();
            } catch (Exception e) {
                LOGGER.error("select now exception", e);
                return (long) -1;
            }
        });
        return results != null && !results.isEmpty() ? results.get(0) : -1;
    }

    @Override
    public Optional<ClientOfflineState> select(String clientId, String subject, String consumerGroup) {
        List<ClientOfflineState> list = jdbcTemplate.query(SELECT_SQL, ROW_MAPPER, clientId, subject, consumerGroup);
        return list != null && !list.isEmpty() ? Optional.ofNullable(list.get(0)) : Optional.empty();
    }

    @Override
    public List<ClientOfflineState> selectAll() {
        return jdbcTemplate.query(SELECT_ALL_SQL, ROW_MAPPER);
    }

    @Override
    public void insertOrUpdate(ClientOfflineState clientState) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(INSERT_SQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, clientState.getClientId());
            ps.setString(2, clientState.getSubject());
            ps.setString(3, clientState.getConsumerGroup());
            ps.setInt(4, clientState.getState().code());
            ps.setInt(5, clientState.getState().code());
            return ps;
        });
    }

    @Override
    public void delete(String subject, String consumerGroup) {
        jdbcTemplate.update(DELETE_BY_SUBJECT_GROUP_SQL, subject, consumerGroup);
    }

    @Override
    public void delete(String clientId, String subject, String consumerGroup) {
        jdbcTemplate.update(DELETE_SQL, clientId, subject, consumerGroup);
    }
}
