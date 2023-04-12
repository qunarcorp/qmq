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

import static qunar.tc.qmq.meta.store.impl.Serializer.serialize;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerGroupKind;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.utils.CharsetUtils;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

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

    private static final String FIND_BROKER_GROUP_SQL = "SELECT group_name, kind, master_address, broker_state, tag, ext,update_time FROM broker_group";
    private static final String SELECT_BROKER_GROUP_SQL = "SELECT group_name, kind, master_address, broker_state, tag,ext, update_time FROM broker_group WHERE group_name = ?";
    private static final String UPDATE_BROKER_GROUP_SQL = "UPDATE broker_group SET broker_state = ? WHERE group_name = ?";
    private static final String UPDATE_BROKER_GROUP_TAG_SQL = "UPDATE broker_group SET tag = ? WHERE group_name = ?";
    private static final String UPDATE_BROKER_GROUP_EXT_SQL = "UPDATE broker_group SET ext = ? WHERE group_name = ?";
    private static final String INSERT_OR_UPDATE_BROKER_GROUP_SQL = "INSERT INTO broker_group(group_name,kind,master_address,broker_state,create_time) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE master_address=?,broker_state=?";

    private static final String INSERT_CLIENT_META_INFO_SQL = "INSERT IGNORE INTO client_meta_info(subject_info,client_type,consumer_group,client_id,app_code,room,create_time) VALUES(?, ?, ?, ?, ?, ?,?)";

    private static final String INSERT_OR_UPDATE_CLIENT_META_INFO_SQL = "UPDATE client_meta_info SET update_time = ? WHERE client_id =? and subject_info in (?)";

    private static final String SELECT_CLIENT_META_INFO_BY_CLIENT_IDS_SQL = "SELECT id ,subject_info,client_type,consumer_group,client_id,app_code,room,create_time,update_time from   client_meta_info where client_id in (?)";

    private static final String SELECT_CLIENT_META_INFO_BY_SUBJECTS_SQL ="SELECT id ,subject_info,client_type,consumer_group,client_id,app_code,room,create_time,update_time  from  client_meta_info where client_id =:clientId and subject_info in (:subjectInfo)";

    private static final String SELECT_CLIENT_META_INFO_BY_APP_SQL = "SELECT id ,subject_info,client_type,consumer_group,client_id,app_code,room,create_time,update_time  from   client_meta_info where app_code = ? and subject_info =?";

    private static final String SELECT_CLIENT_META_INFO_BY_SUBJECT_SQL = "SELECT id ,subject_info,client_type,consumer_group,client_id,app_code,room,create_time,update_time  from   client_meta_info where subject_info =?";

    private static final String INSERT_SUBJECT_INFO_SQL = "INSERT INTO subject_info(name,tag,create_time) VALUES(?,?,?)";
    private static final String ALL_SUBJECT_INFO_SQL = "SELECT name, tag, update_time FROM subject_info";
    private static final String QUERY_SUBJECT_INFO_SQL = "SELECT name, tag, update_time FROM subject_info WHERE name=?";
    private static final String UPDATE_CLIENT_META_INFO_SQL = "UPDATE client_meta_info SET update_time = NOW() WHERE  id in (%s)";


    public static final Joiner JOINER_COMMA = Joiner.on(",");

    private static final RowMapper<BrokerGroup> BROKER_GROUP_ROW_MAPPER = (rs, rowNum) -> {
        final BrokerGroup brokerGroup = new BrokerGroup();
        brokerGroup.setGroupName(rs.getString("group_name"));
        brokerGroup.setMaster(rs.getString("master_address"));
        brokerGroup.setBrokerState(BrokerState.codeOf(rs.getInt("broker_state")));
        brokerGroup.setUpdateTime(rs.getTimestamp("update_time").getTime());
        brokerGroup.setTag(rs.getString("tag"));
        final String extJSON = rs.getString("ext");
        if (CharsetUtils.hasText(extJSON)) {
            final Map<String, String> map = Serializer.deSerialize(extJSON, new TypeReference<Map<String, String>>() {
            });
            brokerGroup.setExt(map);
        }
        brokerGroup.setKind(BrokerGroupKind.fromCode(rs.getInt("kind")));

        return brokerGroup;
    };

    private static final RowMapper<SubjectRoute> SUBJECT_ROUTE_ROW_MAPPER = (rs, rowNum) -> {
        final String subject = rs.getString("subject_info");
        final String groupInfoJson = rs.getString("broker_group_json");
        final Timestamp updateTime = rs.getTimestamp("update_time");
        final int version = rs.getInt("version");
        final List<String> groupNames = Serializer.deSerialize(groupInfoJson, new TypeReference<List<String>>() {
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

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate nPjdbcTemplate;

    public DatabaseStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.nPjdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate.getDataSource());
    }

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
    public void updateBrokerGroupExt(String groupName, Map<String,String> ext) {
        final String extJson = serialize(ext);
        jdbcTemplate.update(UPDATE_BROKER_GROUP_EXT_SQL, extJson, groupName);
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
                request.getConsumerGroup(), request.getClientId(), request.getAppCode(), request.getClientLdc(), now);
    }

    private static final RowMapper<ClientMetaInfo> CLIENT_META_INFO_ROW_MAPPER= (rs, rowNum) -> {
        final ClientMetaInfo clientMetaInfo = new ClientMetaInfo();
        clientMetaInfo.setId(rs.getInt("id"));
        clientMetaInfo.setSubject(rs.getString("subject_info"));
        clientMetaInfo.setClientTypeCode(rs.getInt("client_type"));
        clientMetaInfo.setConsumerGroup(rs.getString("consumer_group"));
        clientMetaInfo.setClientId((rs.getString("client_id")));
        clientMetaInfo.setAppCode(rs.getString("app_code"));
        clientMetaInfo.setRoom(rs.getString("room"));
        clientMetaInfo.setCreateTime(rs.getTimestamp("create_time"));
        clientMetaInfo.setUpdateTime(rs.getTimestamp("update_time"));
        return clientMetaInfo;
    };



    @Override
    public List<ClientMetaInfo> queryClientIds(List<String> clientIds) {
        return jdbcTemplate.query(SELECT_CLIENT_META_INFO_BY_CLIENT_IDS_SQL, CLIENT_META_INFO_ROW_MAPPER, JOINER_COMMA.join(clientIds));
    }

    @Override
    public List<ClientMetaInfo> queryConsumerByIdAndSubject(String clientId, Set<String> subjects) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("clientId",clientId);
        params.addValue("subjectInfo",subjects);
        return nPjdbcTemplate.query(SELECT_CLIENT_META_INFO_BY_SUBJECTS_SQL, params, CLIENT_META_INFO_ROW_MAPPER);
    }

    @Override
    public List<ClientMetaInfo> queryConsumerByAppAndSubject(String appCode, String subject) {
        return jdbcTemplate.query(SELECT_CLIENT_META_INFO_BY_APP_SQL, CLIENT_META_INFO_ROW_MAPPER, appCode, subject);
    }

    @Override
    public List<ClientMetaInfo> queryConsumerBySubject(String subject) {
        return jdbcTemplate.query(SELECT_CLIENT_META_INFO_BY_SUBJECT_SQL, CLIENT_META_INFO_ROW_MAPPER, subject);
    }

    @Override
    public int updateTimeByIds(List<Long> ids) {
        String inSql = String.join(",", Collections.nCopies(ids.size(),"?"));
        return jdbcTemplate.update(String.format(UPDATE_CLIENT_META_INFO_SQL, inSql), ids.toArray());
    }

    @Override
    public void batchUpdateMetaInfo(String clientId ,String subjects) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(INSERT_OR_UPDATE_CLIENT_META_INFO_SQL, now, clientId, subjects);
    }
}
