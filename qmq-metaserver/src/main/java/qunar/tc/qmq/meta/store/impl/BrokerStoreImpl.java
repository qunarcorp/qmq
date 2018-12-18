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
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.meta.model.BrokerMeta;
import qunar.tc.qmq.meta.store.BrokerStore;

import java.util.List;
import java.util.Optional;

/**
 * @author keli.wang
 * @since 2018-11-29
 */
public class BrokerStoreImpl implements BrokerStore {
    private static final String QUERY_BROKER_SQL = "SELECT group_name,role,hostname,INET_NTOA(ip) AS ip,serve_port,sync_port FROM broker WHERE hostname=? AND serve_port=?";
    private static final String QUERY_BY_ROLE_SQL = "SELECT group_name,role,hostname,INET_NTOA(ip) AS ip,serve_port,sync_port FROM broker WHERE group_name=? AND role=?";
    private static final String QUERY_BROKERS_SQL = "SELECT group_name,role,hostname,INET_NTOA(ip) AS ip,serve_port,sync_port FROM broker WHERE group_name=?";
    private static final String ALL_BROKERS_SQL = "SELECT group_name,role,hostname,INET_NTOA(ip) AS ip,serve_port,sync_port FROM broker";
    private static final String INSERT_BROKER_SQL = "INSERT IGNORE INTO broker(group_name,role,hostname,ip,serve_port,sync_port) VALUES (?,?,?,INET_ATON(?),?,?)";
    private static final String REPLACE_BROKER_BY_ROLE_SQL = "UPDATE broker SET hostname=?,ip=INET_ATON(?),serve_port=?,sync_port=? WHERE group_name=? AND role=? AND hostname=? AND serve_port=INET_ATON(?)";

    private static final RowMapper<BrokerMeta> MAPPER = (rs, i) -> {
        final String group = rs.getString("group_name");
        final BrokerRole role = BrokerRole.fromCode(rs.getInt("role"));
        final String hostname = rs.getString("hostname");
        final String ip = rs.getString("ip");
        final int servePort = rs.getInt("serve_port");
        final int syncPort = rs.getInt("sync_port");
        return new BrokerMeta(group, role, hostname, ip, servePort, syncPort);
    };

    private final JdbcTemplate jdbcTemplate;

    public BrokerStoreImpl(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Optional<BrokerMeta> queryBroker(final String hostname, final int servePort) {
        final List<BrokerMeta> brokers = jdbcTemplate.query(QUERY_BROKER_SQL, MAPPER, hostname, servePort);
        return optionalBroker(brokers);
    }

    @Override
    public Optional<BrokerMeta> queryByRole(final String brokerGroup, final int role) {
        final List<BrokerMeta> brokers = jdbcTemplate.query(QUERY_BY_ROLE_SQL, MAPPER, brokerGroup, role);
        return optionalBroker(brokers);
    }

    private Optional<BrokerMeta> optionalBroker(final List<BrokerMeta> brokers) {
        if (brokers.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(brokers.get(0));
        }
    }

    @Override
    public List<BrokerMeta> queryBrokers(final String groupName) {
        return jdbcTemplate.query(QUERY_BROKERS_SQL, MAPPER, groupName);
    }

    @Override
    public List<BrokerMeta> allBrokers() {
        return jdbcTemplate.query(ALL_BROKERS_SQL, MAPPER);
    }

    @Override
    public int insertBroker(final BrokerMeta broker) {
        return jdbcTemplate.update(INSERT_BROKER_SQL,
                broker.getGroup(), broker.getRole().getCode(),
                broker.getHostname(), broker.getIp(), broker.getServePort(), broker.getSyncPort());
    }

    @Override
    public int replaceBrokerByRole(final BrokerMeta oldBroker, final BrokerMeta newBroker) {
        return jdbcTemplate.update(REPLACE_BROKER_BY_ROLE_SQL,
                newBroker.getHostname(), newBroker.getIp(), newBroker.getServePort(), newBroker.getSyncPort(),
                oldBroker.getGroup(), oldBroker.getRole(), oldBroker.getHostname(), oldBroker.getSyncPort());
    }
}
