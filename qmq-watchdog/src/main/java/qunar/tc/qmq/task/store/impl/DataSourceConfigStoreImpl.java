package qunar.tc.qmq.task.store.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.task.model.DataSourceInfoModel;
import qunar.tc.qmq.task.model.DataSourceInfoStatus;
import qunar.tc.qmq.task.store.IDataSourceConfigStore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by IntelliJ IDEA. User: liuzz Date: 12-12-21 Time: 下午8:04
 */
public class DataSourceConfigStoreImpl implements IDataSourceConfigStore {
    private static final String SELECT_SQL = "SELECT url,user_name,password,status,room,update_time FROM datasource_config WHERE status=? AND room=?";

    private static final DatasourceInfoMapping DATASOURCE_INFO_MAPPING = new DatasourceInfoMapping();

    private JdbcTemplate jdbcTemplate;

    public DataSourceConfigStoreImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<DataSourceInfoModel> findDataSourceInfos(DataSourceInfoStatus status, String namespace) {
        return jdbcTemplate.query(SELECT_SQL, DATASOURCE_INFO_MAPPING, status.code, namespace);
    }

    private static class DatasourceInfoMapping implements RowMapper<DataSourceInfoModel> {
        @Override
        public DataSourceInfoModel mapRow(ResultSet rs, int rowNum) throws SQLException {
            final DataSourceInfoModel model = new DataSourceInfoModel();
            model.setUrl(rs.getString("url"));
            model.setStatus(rs.getInt("status"));
            model.setUpdateTime(rs.getTimestamp("update_time"));
            model.setRoom(rs.getString("room"));
            model.setUserName(rs.getString("user_name"));
            model.setPassword(rs.getString("password"));
            return model;
        }
    }
}
