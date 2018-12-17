package qunar.tc.qmq.task.store.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.task.database.DatasourceWrapper;
import qunar.tc.qmq.task.model.MsgQueue;
import qunar.tc.qmq.task.store.IMessageClientStore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * Created by zhaohui.yu
 * 16/3/28
 */
public class MessageClientStore implements IMessageClientStore {
    private static MsgQueue toMsgQueue(ResultSet rs, int rowNum) throws SQLException {
        return new MsgQueue(
                rs.getLong("id"),
                rs.getString("content"),
                rs.getInt("error"),
                rs.getTimestamp("update_time"));
    }

    @Override
    public List<MsgQueue> findErrorMsg(DatasourceWrapper dataSource, Date since) {
        return create(dataSource).query(dataSource.select_sql(), MessageClientStore::toMsgQueue, since);
    }

    @Override
    public void deleteByMessageId(DatasourceWrapper dataSource, long messageId) {
        create(dataSource).update(dataSource.delete_sql(), messageId);
    }

    @Override
    public void updateError(DatasourceWrapper dataSource, long messageId, int state) {
        create(dataSource).update(dataSource.error_sql(), state, new Timestamp(System.currentTimeMillis()), messageId);
    }

    protected JdbcTemplate create(DatasourceWrapper dataSource) {
        return new JdbcTemplate(dataSource.datasource());
    }
}
