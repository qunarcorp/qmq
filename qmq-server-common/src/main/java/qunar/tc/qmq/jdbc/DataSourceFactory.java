package qunar.tc.qmq.jdbc;

import javax.sql.DataSource;

import qunar.tc.qmq.configuration.DynamicConfig;

public interface DataSourceFactory {
	DataSource createDataSource(DynamicConfig config);
}
