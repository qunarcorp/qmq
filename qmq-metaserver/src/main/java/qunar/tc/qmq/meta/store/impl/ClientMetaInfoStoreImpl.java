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
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;

import java.util.List;

/**
 * @author keli.wang
 * @since 2017/12/5
 */
public class ClientMetaInfoStoreImpl implements ClientMetaInfoStore {
    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();

    @Override
    public List<ClientMetaInfo> queryConsumer(String subject) {
        return jdbcTemplate.query("SELECT subject_info,client_type,consumer_group,client_id,app_code,room FROM client_meta_info WHERE subject_info=? AND client_type=?", (rs, rowNum) -> {
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
}
