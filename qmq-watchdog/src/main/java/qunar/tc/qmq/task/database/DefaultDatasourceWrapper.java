package qunar.tc.qmq.task.database;

import javax.sql.DataSource;

/**
 * Created by zhaohui.yu
 * 12/6/16
 */
public class DefaultDatasourceWrapper implements DatasourceWrapper {

    private static final String SELECT_SQL = "SELECT id,content,error,update_time FROM qmq_produce.qmq_msg_queue WHERE status=0 AND update_time<? ORDER BY id ASC LIMIT 50";
    private static final String DELETE_SQL = "DELETE FROM qmq_produce.qmq_msg_queue WHERE id=?";
    private static final String ERROR_SQL = "UPDATE qmq_produce.qmq_msg_queue SET status=?,error=error+1,update_time=? WHERE id=?";

    private final DataSource dataSource;

    public DefaultDatasourceWrapper(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public DataSource datasource() {
        return dataSource;
    }

    @Override
    public String select_sql() {
        return SELECT_SQL;
    }

    @Override
    public String delete_sql() {
        return DELETE_SQL;
    }

    @Override
    public String error_sql() {
        return ERROR_SQL;
    }
}
