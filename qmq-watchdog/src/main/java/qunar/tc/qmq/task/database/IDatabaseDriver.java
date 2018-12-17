package qunar.tc.qmq.task.database;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 13-12-5
 * Time: 下午3:01
 */
public interface IDatabaseDriver {
    DatasourceWrapper makeDataSource(String url, String userName, String password);

    void close(DatasourceWrapper dataSource);
}
