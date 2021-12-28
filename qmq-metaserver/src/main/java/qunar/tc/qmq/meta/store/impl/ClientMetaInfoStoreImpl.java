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

import java.util.List;

import com.google.common.base.Joiner;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * @author keli.wang
 * @since 2017/12/5
 */
public class ClientMetaInfoStoreImpl implements ClientMetaInfoStore {

    public static final Joiner JOINER_COMMA = Joiner.on(",");

    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();


    private static final RowMapper<ClientMetaInfo> CLIENT_META_INFO_ROW_MAPPER= (rs, rowNum) -> {
        final ClientMetaInfo clientMetaInfo = new ClientMetaInfo();
        clientMetaInfo.setId(rs.getInt("id"));
        clientMetaInfo.setSubject(rs.getString("subject_info"));
        clientMetaInfo.setClientTypeCode(rs.getInt("client_type"));
        clientMetaInfo.setConsumerGroup(rs.getString("consumer_group"));
        clientMetaInfo.setClientId((rs.getString("client_id")));
        clientMetaInfo.setAppCode(rs.getString("app_code"));
        clientMetaInfo.setRoom(rs.getString("room"));
        clientMetaInfo.setCreateTime(rs.getDate("create_time"));
        clientMetaInfo.setUpdateTime(rs.getDate("update_time"));
        return clientMetaInfo;
    };

    @Override
    public List<ClientMetaInfo> queryConsumer(String subject) {
        return jdbcTemplate.query("SELECT id,subject_info,client_type,consumer_group,client_id,app_code,room FROM client_meta_info WHERE subject_info=? AND client_type=?", (rs, rowNum) -> {
            final ClientMetaInfo meta = new ClientMetaInfo();
            meta.setSubject(rs.getString("subject_info"));
            meta.setClientTypeCode(rs.getInt("client_type"));
            meta.setConsumerGroup(rs.getString("consumer_group"));
            meta.setClientId(rs.getString("client_id"));
            meta.setAppCode(rs.getString("app_code"));
            meta.setRoom(rs.getString("room"));
            return meta;
        }, subject, ClientType.CONSUMER.getCode());
    }


    @Override
    public List<ClientMetaInfo> queryClientIds(List<String> clientIds) {
        return jdbcTemplate.query("SELECT id ,subject_info,client_type,consumer_group,client_id,app_code,room,create_time,update_time from   client_meta_info where client_id in (?)", CLIENT_META_INFO_ROW_MAPPER, JOINER_COMMA.join(clientIds));
    }


    @Override
    public int updateTimeByIds(List<String> ids) {
        return jdbcTemplate.update("UPDATE client_meta_info SET update_time = ? WHERE  id in (?)", JOINER_COMMA.join(ids));
    }
}
