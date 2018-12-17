package qunar.tc.qmq.task.database;


import java.net.URI;

/**
 * Created by IntelliJ IDEA. User: liuzz Date: 13-12-5 Time: 下午3:01
 */
public class MysqlMMMDatabaseDriver implements IDatabaseDriver {
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    private final IDataSourceService dataSourceService;


    public MysqlMMMDatabaseDriver() {
        dataSourceService = new TomcatDataSourceService();
    }

    public DatasourceWrapper makeDataSource(String url, String userName, String password) {
        String jdbcUrl = covertToJdbcUrl(url);
        return new DefaultDatasourceWrapper(dataSourceService.makeDataSource(jdbcUrl, DRIVER_CLASS, userName, password));
    }

    private String covertToJdbcUrl(String url) {
        URI uri = URI.create(url);
        return String.format("jdbc:mysql://%s:%s", uri.getHost(), uri.getPort());
    }

    @Override
    public void close(DatasourceWrapper dataSource) {
        if (dataSource == null) return;
        dataSourceService.close(dataSource.datasource());
    }
}
