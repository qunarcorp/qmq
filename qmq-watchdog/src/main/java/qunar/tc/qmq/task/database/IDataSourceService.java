package qunar.tc.qmq.task.database;

import javax.sql.DataSource;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 12-12-21
 * Time: 下午2:15
 */
public interface IDataSourceService {
    DataSource makeDataSource(String url, String driverClassName, String userName, String pwd);

    void close(DataSource dataSource);
}
