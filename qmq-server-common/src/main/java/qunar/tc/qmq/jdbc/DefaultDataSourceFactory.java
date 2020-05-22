package qunar.tc.qmq.jdbc;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import qunar.tc.qmq.configuration.DynamicConfig;

public class DefaultDataSourceFactory implements DataSourceFactory {

	@Override
	public DataSource createDataSource(DynamicConfig config) {
		final HikariConfig cpConfig = new HikariConfig();
		cpConfig.setDriverClassName(config.getString("jdbc.driverClassName", "com.mysql.jdbc.Driver"));
		cpConfig.setJdbcUrl(config.getString("jdbc.url"));
		cpConfig.setUsername(config.getString("jdbc.username"));
		cpConfig.setPassword(config.getString("jdbc.password"));
		cpConfig.setMaximumPoolSize(config.getInt("pool.size.max", 10));

		return new HikariDataSource(cpConfig);
	}
}
