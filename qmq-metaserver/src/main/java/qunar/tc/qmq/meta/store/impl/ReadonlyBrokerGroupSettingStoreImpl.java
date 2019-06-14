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
import qunar.tc.qmq.meta.model.ReadonlyBrokerGroupSetting;
import qunar.tc.qmq.meta.store.ReadonlyBrokerGroupSettingStore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author keli.wang
 * @since 2018/7/30
 */
public class ReadonlyBrokerGroupSettingStoreImpl implements ReadonlyBrokerGroupSettingStore {
    private static final String INSERT_SQL = "INSERT INTO `readonly_broker_group_setting`(`subject`, `broker_group`) VALUES (?,?)";
    private static final String DELETE_SQL = "DELETE FROM `readonly_broker_group_setting` WHERE `subject`=? AND `broker_group`=?";
    private static final String ALL_READONLY_BROKER_GROUPS = "SELECT `subject`,`broker_group` FROM `readonly_broker_group_setting`";

    private final JdbcTemplate jdbcTemplate;

    public ReadonlyBrokerGroupSettingStoreImpl(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public int insert(final ReadonlyBrokerGroupSetting setting) {
        return jdbcTemplate.update(INSERT_SQL, setting.getSubject(), setting.getBrokerGroup());
    }

    @Override
    public int delete(final ReadonlyBrokerGroupSetting setting) {
        return jdbcTemplate.update(DELETE_SQL, setting.getSubject(), setting.getBrokerGroup());
    }

    @Override
    public List<ReadonlyBrokerGroupSetting> allReadonlyBrokerGroupSettings() {
        return jdbcTemplate.query(ALL_READONLY_BROKER_GROUPS, this::mapRow);
    }

    private ReadonlyBrokerGroupSetting mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final String subject = rs.getString("subject");
        final String brokerGroup = rs.getString("broker_group");
        return new ReadonlyBrokerGroupSetting(subject, brokerGroup);
    }
}
