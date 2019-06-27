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

import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.model.ClientDbInfo;
import qunar.tc.qmq.meta.store.ClientDbConfigurationStore;

import java.sql.Timestamp;

public class ClientDbConfigurationStoreImpl implements ClientDbConfigurationStore {

    private final JdbcTemplate jdbcTemplate;

    public ClientDbConfigurationStoreImpl() {
        this.jdbcTemplate = JdbcTemplateHolder.getOrCreate();
    }

    @Override
    public void insertDb(ClientDbInfo clientDbInfo) {
        jdbcTemplate.update("INSERT IGNORE INTO datasource_config(url,user_name,password,room,create_time) VALUES(?,?,?,?,?)",
                clientDbInfo.getUrl(), clientDbInfo.getUserName(), clientDbInfo.getPassword(), clientDbInfo.getRoom(), new Timestamp(System.currentTimeMillis()));
    }
}
