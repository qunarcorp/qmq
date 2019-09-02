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

import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerGroupKind;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static qunar.tc.qmq.meta.store.impl.JsonUtils.serialize;

/**
 * @author yunfeng.yang
 * @since 2017/8/31
 */
public class DatabaseStore implements Store {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseStore.class);

    private static final String INSERT_SUBJECT_ROUTE_SQL = "INSERT IGNORE INTO subject_route(subject_info, version, broker_group_json, create_time) VALUES(?, ?, ?, ?)";
    private static final String UPDATE_SUBJECT_ROUTE_SQL = "UPDATE subject_route SET broker_group_json = ?, version = version + 1 WHERE subject_info = ? AND version = ?";
    private static final String FIND_SUBJECT_ROUTE_SQL = "SELECT subject_info, version, broker_group_json, update_time FROM subject_route";
    private static final String SELECT_SUBJECT_ROUTE_SQL = "SELECT subject_info, version, broker_group_json, update_time FROM subject_route WHERE subject_info = ?";

    private static final String FIND_BROKER_GROUP_SQL = "SELECT group_name, kind, master_address, broker_state, tag, update_time FROM broker_group";
    private static final String SELECT_BROKER_GROUP_SQL = "SELECT group_name, kind, master_address, broker_state, tag, update_time FROM broker_group WHERE group_name = ?";
    private static final String UPDATE_BROKER_GROUP_SQL = "UPDATE broker_group SET broker_state = ? WHERE group_name = ?";
    private static final String UPDATE_BROKER_GROUP_TAG_SQL = "UPDATE broker_group SET tag = ? WHERE group_name = ?";
    private static final String INSERT_OR_UPDATE_BROKER_GROUP_SQL = "INSERT INTO broker_group(group_name,kind,master_address,broker_state,create_time) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE master_address=?,broker_state=?";

    private static final String INSERT_CLIENT_META_INFO_SQL = "INSERT INTO client_meta_info(subject_info,client_type,consumer_group,client_id,app_code,create_time) VALUES(?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE online_status = ?, update_time = now()";

    private static final String INSERT_SUBJECT_INFO_SQL = "INSERT INTO subject_info(name,tag,create_time) VALUES(?,?,?)";
    private static final String ALL_SUBJECT_INFO_SQL = "SELECT name, tag, update_time FROM subject_info";
    private static final String QUERY_SUBJECT_INFO_SQL = "SELECT name, tag, update_time FROM subject_info WHERE name=?";

    private static final RowMapper<BrokerGroup> BROKER_GROUP_ROW_MAPPER = (rs, rowNum) -> {
        final BrokerGroup brokerGroup = new BrokerGroup();
        brokerGroup.setGroupName(rs.getString("group_name"));
        brokerGroup.setMaster(rs.getString("master_address"));
        brokerGroup.setBrokerState(BrokerState.codeOf(rs.getInt("broker_state")));
        brokerGroup.setUpdateTime(rs.getTimestamp("update_time").getTime());
        brokerGroup.setTag(rs.getString("tag"));
        brokerGroup.setKind(BrokerGroupKind.fromCode(rs.getInt("kind")));

        return brokerGroup;
    };
    private static final RowMapper<SubjectRoute> SUBJECT_ROUTE_ROW_MAPPER = (rs, rowNum) -> {
        final String subject = rs.getString("subject_info");
        final String groupInfoJson = rs.getString("broker_group_json");
        final Timestamp updateTime = rs.getTimestamp("update_time");
        final int version = rs.getInt("version");
        final List<String> groupNames = JsonUtils.deSerialize(groupInfoJson, new TypeReference<List<String>>() {
        });
        final SubjectRoute subjectRoute = new SubjectRoute();
        subjectRoute.setSubject(subject);
        subjectRoute.setVersion(version);
        subjectRoute.setBrokerGroups(groupNames);
        subjectRoute.setUpdateTime(updateTime.getTime());
        return subjectRoute;
    };
    private static final RowMapper<SubjectInfo> SUBJECT_INFO_ROW_MAPPER = (rs, rowNum) -> {
        final SubjectInfo subjectInfo = new SubjectInfo();
        subjectInfo.setName(rs.getString("name"));
        subjectInfo.setTag(rs.getString("tag"));
        subjectInfo.setUpdateTime(rs.getTimestamp("update_time").getTime());

        return subjectInfo;
    };

    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();

    @Override
    public int insertSubjectRoute(String subject, int version, List<String> groupNames) {
        final String content = serialize(groupNames);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        return jdbcTemplate.update(INSERT_SUBJECT_ROUTE_SQL, subject, version, content, now);
    }

    @Override
    public int updateSubjectRoute(String subject, int version, List<String> groupNames) {
        final String serialize = serialize(groupNames);
        return jdbcTemplate.update(UPDATE_SUBJECT_ROUTE_SQL, serialize, subject, version);
    }

    @Override
    public SubjectRoute selectSubjectRoute(String subject) {
        return jdbcTemplate.queryForObject(SELECT_SUBJECT_ROUTE_SQL, SUBJECT_ROUTE_ROW_MAPPER, subject);
    }

    @Override
    public void insertOrUpdateBrokerGroup(final String groupName, final BrokerGroupKind kind, final String masterAddress, final BrokerState brokerState) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(INSERT_OR_UPDATE_BROKER_GROUP_SQL, groupName, kind.getCode(), masterAddress, brokerState.getCode(), now, masterAddress, brokerState.getCode());
    }

    @Override
    public void updateBrokerGroup(String groupName, BrokerState brokerState) {
        jdbcTemplate.update(UPDATE_BROKER_GROUP_SQL, brokerState.getCode(), groupName);
    }

    @Override
    public void updateBrokerGroupTag(String groupName, String tag) {
        jdbcTemplate.update(UPDATE_BROKER_GROUP_TAG_SQL, tag, groupName);
    }

    @Override
    public void insertSubject(String subject, String tag) {
        jdbcTemplate.update(INSERT_SUBJECT_INFO_SQL, subject, tag, new Date());
    }

    @Override
    public List<BrokerGroup> getAllBrokerGroups() {
        return jdbcTemplate.query(FIND_BROKER_GROUP_SQL, BROKER_GROUP_ROW_MAPPER);
    }

    @Override
    public List<SubjectRoute> getAllSubjectRoutes() {
        try {
            return jdbcTemplate.query(FIND_SUBJECT_ROUTE_SQL, SUBJECT_ROUTE_ROW_MAPPER);
        } catch (Exception e) {
            LOG.error("getAllSubjectRoutes error", e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<SubjectInfo> getAllSubjectInfo() {
        try {
            return jdbcTemplate.query(ALL_SUBJECT_INFO_SQL, SUBJECT_INFO_ROW_MAPPER);
        } catch (Exception e) {
            LOG.error("getAllSubjectInfo error", e);
            return Collections.emptyList();
        }
    }

    @Override
    public SubjectInfo getSubjectInfo(String subject) {
        try {
            return jdbcTemplate.queryForObject(QUERY_SUBJECT_INFO_SQL, SUBJECT_INFO_ROW_MAPPER, subject);
        } catch (Exception e) {
            LOG.error("getAllSubjectInfo error", e);
            return null;
        }
    }

    @Override
    public BrokerGroup getBrokerGroup(String groupName) {
        try {
            return jdbcTemplate.queryForObject(SELECT_BROKER_GROUP_SQL, BROKER_GROUP_ROW_MAPPER, groupName);
        } catch (EmptyResultDataAccessException ignore) {
            return null;
        }
    }

    @Override
    public void insertClientMetaInfo(MetaInfoRequest request) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(INSERT_CLIENT_META_INFO_SQL, request.getSubject(), request.getClientTypeCode(),
                request.getConsumerGroup(), request.getClientId(), request.getAppCode(), now, request.getOnlineState().name());
    }
}
