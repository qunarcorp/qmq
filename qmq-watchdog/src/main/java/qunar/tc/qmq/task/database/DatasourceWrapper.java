package qunar.tc.qmq.task.database;

import javax.sql.DataSource;

/**
 * Created by zhaohui.yu
 * 12/6/16
 */
public interface DatasourceWrapper {
    DataSource datasource();

    String select_sql();

    String delete_sql();

    String error_sql();
}
