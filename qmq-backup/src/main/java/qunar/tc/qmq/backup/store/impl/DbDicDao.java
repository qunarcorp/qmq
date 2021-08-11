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

package qunar.tc.qmq.backup.store.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.backup.store.DicStore;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_DB_DIC_TABLE;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_DELAY_DB_DIC_TABLE;

/**
 * @author yiqun.fan create on 17-10-31.
 */
public class DbDicDao implements DicStore {
    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();
    private final String getIdSql;
    private final String getNameSql;
    private final String insertNameSql;

    public DbDicDao(boolean isDelay) {
        String table = !isDelay ? DEFAULT_DB_DIC_TABLE : DEFAULT_DELAY_DB_DIC_TABLE;
        getIdSql = String.format("SELECT name_id FROM %s WHERE name =?", table);
        getNameSql = String.format("SELECT name FROM %s WHERE name_id=?", table);
        insertNameSql = String.format("INSERT INTO %s(name_id,name) VALUES(?,?)", table);
    }

    @Override
    public String getName(int nameId) {
        return jdbcTemplate.queryForObject(getNameSql, new Object[]{nameId}, String.class);
    }

    @Override
    public int getNameId(String name) {
        return jdbcTemplate.queryForObject(getIdSql, new Object[]{name}, Integer.class);
    }

    @Override
    public void insertName(int nameId, String name) {
        jdbcTemplate.update(insertNameSql, nameId, name);
    }
}
